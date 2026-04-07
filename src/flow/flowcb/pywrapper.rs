//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::Worklist;
use crate::flow::flowcb::FlowCB;
use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

pub struct PyWrapperPlugin {
    name: String,
    instance: PyObject,
}

impl PyWrapperPlugin {
    pub fn new(factory_path: &str, _config: &Config) -> anyhow::Result<Self> {
        let instance = Python::with_gil(|py| -> PyResult<PyObject> {
            let sys = py.import("sys")?;
            let path = sys.getattr("path")?;
            
            let plugins_dir = crate::config::paths::get_user_config_dir().join("plugins");
            if let Some(plugins_str) = plugins_dir.to_str() {
                path.call_method1("append", (plugins_str,))?;
            }

            path.call_method1("append", (".",))?;

            let (module_name, class_name) = if !factory_path.contains('.') {
                let mut chars = factory_path.chars();
                let capitalized = match chars.next() {
                    None => String::new(),
                    Some(f) => f.to_uppercase().collect::<String>() + chars.as_str(),
                };
                (factory_path, capitalized)
            } else {
                let parts: Vec<&str> = factory_path.rsplitn(2, '.').collect();
                (parts[1], parts[0].to_string())
            };

            ::log::debug!("Loading Python plugin module '{}', class '{}'", module_name, class_name);

            let module = match py.import(module_name) {
                Ok(m) => m,
                Err(e) => {
                    let fallback = format!("sarracenia.flowcb.{}", module_name);
                    py.import(fallback.as_str()).map_err(|_| e)?
                }
            };

            let class = module.getattr(class_name.as_str())?;
            
            let options = PyDict::new(py);
            options.set_item("component", &_config.component)?;
            options.set_item("config", _config.configname.clone().unwrap_or_else(|| "unknown".to_string()))?;
            
            let instance = class.call1((options,))?;
            
            Ok(instance.into())
        }).map_err(|e| anyhow::anyhow!("Failed to load Python plugin {}: {:?}", factory_path, e))?;

        Ok(Self {
            name: factory_path.to_string(),
            instance,
        })
    }

    fn msg_to_py<'py>(&self, py: Python<'py>, msg: &Message) -> PyResult<&'py PyDict> {
        let dict = PyDict::new(py);
        dict.set_item("baseUrl", &msg.base_url)?;
        dict.set_item("relPath", &msg.rel_path)?;
        dict.set_item("pubTime", msg.pub_time.to_rfc3339())?;
        
        for (k, v) in &msg.fields {
            dict.set_item(k, v)?;
        }
        Ok(dict)
    }

    fn py_to_msg(&self, dict: &PyDict) -> PyResult<Message> {
        let base_url: String = dict.get_item("baseUrl")?.unwrap().extract()?;
        let rel_path: String = dict.get_item("relPath")?.unwrap().extract()?;
        let mut msg = Message::new(&base_url, &rel_path);
        
        for (k, v) in dict.iter() {
            let k_str: String = k.extract()?;
            if k_str != "baseUrl" && k_str != "relPath" {
                if let Ok(v_str) = v.extract::<String>() {
                    msg.fields.insert(k_str, v_str);
                }
            }
        }
        Ok(msg)
    }

    fn vec_to_py_list<'py>(&self, py: Python<'py>, vec: &[Message]) -> PyResult<&'py PyList> {
        let list = PyList::empty(py);
        for msg in vec {
            list.append(self.msg_to_py(py, msg)?)?;
        }
        Ok(list)
    }

    fn py_list_to_vec(&self, list: &PyAny) -> PyResult<Vec<Message>> {
        let mut vec = Vec::new();
        for item in list.iter()? {
            let dict: &PyDict = item?.downcast()?;
            vec.push(self.py_to_msg(dict)?);
        }
        Ok(vec)
    }
}

#[async_trait]
impl FlowCB for PyWrapperPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn after_accept(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        let incoming = std::mem::take(&mut wl.incoming);
        let ok = std::mem::take(&mut wl.ok);
        let rejected = std::mem::take(&mut wl.rejected);
        let failed = std::mem::take(&mut wl.failed);

        let instance_clone = self.instance.clone();

        let join_handle: tokio::task::JoinHandle<PyResult<(Vec<Message>, Vec<Message>, Vec<Message>, Vec<Message>)>> = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> PyResult<(Vec<Message>, Vec<Message>, Vec<Message>, Vec<Message>)> {
                let locals = PyDict::new(py);
                py.run("class Worklist:\n  pass", None, Some(locals))?;
                let wl_class = locals.get_item("Worklist").unwrap();
                let py_wl = wl_class.expect("Worklist class not found").call0()?;

                let temp_plugin = PyWrapperPlugin { name: String::new(), instance: py.None() };
                
                py_wl.setattr("incoming", temp_plugin.vec_to_py_list(py, &incoming)?)?;
                py_wl.setattr("ok", temp_plugin.vec_to_py_list(py, &ok)?)?;
                py_wl.setattr("rejected", temp_plugin.vec_to_py_list(py, &rejected)?)?;
                py_wl.setattr("failed", temp_plugin.vec_to_py_list(py, &failed)?)?;

                if instance_clone.as_ref(py).hasattr("after_accept")? {
                    instance_clone.call_method1(py, "after_accept", (py_wl,))?;
                }

                let ret_incoming = temp_plugin.py_list_to_vec(py_wl.getattr("incoming")?)?;
                let ret_ok = temp_plugin.py_list_to_vec(py_wl.getattr("ok")?)?;
                let ret_rejected = temp_plugin.py_list_to_vec(py_wl.getattr("rejected")?)?;
                let ret_failed = temp_plugin.py_list_to_vec(py_wl.getattr("failed")?)?;

                Ok((ret_incoming, ret_ok, ret_rejected, ret_failed))
            })
        });

        let (new_incoming, new_ok, new_rejected, new_failed) = join_handle.await.unwrap().map_err(|e| anyhow::anyhow!("Python plugin error: {:?}", e))?;

        wl.incoming = new_incoming;
        wl.ok = new_ok;
        wl.rejected = new_rejected;
        wl.failed = new_failed;

        Ok(())
    }

    async fn after_work(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        let incoming = std::mem::take(&mut wl.incoming);
        let ok = std::mem::take(&mut wl.ok);
        let rejected = std::mem::take(&mut wl.rejected);
        let failed = std::mem::take(&mut wl.failed);

        let instance_clone = self.instance.clone();

        let join_handle: tokio::task::JoinHandle<PyResult<(Vec<Message>, Vec<Message>, Vec<Message>, Vec<Message>)>> = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> PyResult<(Vec<Message>, Vec<Message>, Vec<Message>, Vec<Message>)> {
                let locals = PyDict::new(py);
                py.run("class Worklist:\n  pass", None, Some(locals))?;
                let wl_class = locals.get_item("Worklist").unwrap();
                let py_wl = wl_class.expect("Worklist class not found").call0()?;

                let temp_plugin = PyWrapperPlugin { name: String::new(), instance: py.None() };
                
                py_wl.setattr("incoming", temp_plugin.vec_to_py_list(py, &incoming)?)?;
                py_wl.setattr("ok", temp_plugin.vec_to_py_list(py, &ok)?)?;
                py_wl.setattr("rejected", temp_plugin.vec_to_py_list(py, &rejected)?)?;
                py_wl.setattr("failed", temp_plugin.vec_to_py_list(py, &failed)?)?;

                if instance_clone.as_ref(py).hasattr("after_work")? {
                    instance_clone.call_method1(py, "after_work", (py_wl,))?;
                }

                let ret_incoming = temp_plugin.py_list_to_vec(py_wl.getattr("incoming")?)?;
                let ret_ok = temp_plugin.py_list_to_vec(py_wl.getattr("ok")?)?;
                let ret_rejected = temp_plugin.py_list_to_vec(py_wl.getattr("rejected")?)?;
                let ret_failed = temp_plugin.py_list_to_vec(py_wl.getattr("failed")?)?;

                Ok((ret_incoming, ret_ok, ret_rejected, ret_failed))
            })
        });

        let (new_incoming, new_ok, new_rejected, new_failed) = join_handle.await.unwrap().map_err(|e| anyhow::anyhow!("Python plugin error: {:?}", e))?;

        wl.incoming = new_incoming;
        wl.ok = new_ok;
        wl.rejected = new_rejected;
        wl.failed = new_failed;

        Ok(())
    }

    async fn on_housekeeping(&self, _wl: &mut Worklist) -> anyhow::Result<()> {
        let instance_clone = self.instance.clone();
        let join_handle: tokio::task::JoinHandle<PyResult<()>> = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> PyResult<()> {
                if instance_clone.as_ref(py).hasattr("on_housekeeping")? {
                    instance_clone.call_method0(py, "on_housekeeping")?;
                }
                Ok(())
            })
        });
        join_handle.await.unwrap().map_err(|e| anyhow::anyhow!("Python plugin error: {:?}", e))?;
        Ok(())
    }
}
