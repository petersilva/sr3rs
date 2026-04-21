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

#[pyfunction]
fn rust_log(level: u32, target: String, msg: String) {
    match level {
        50 | 40 => ::log::error!(target: &target, "{}", msg),
        30 => ::log::warn!(target: &target, "{}", msg),
        20 => ::log::info!(target: &target, "{}", msg),
        10 => ::log::debug!(target: &target, "{}", msg),
        _ => ::log::trace!(target: &target, "{}", msg),
    }
}

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

            path.call_method1("append", ("/home/peter/Sarracenia/sr3",))?;
            path.call_method1("append", ("/home/peter/Sarracenia/sr3/sarracenia",))?;
            path.call_method1("append", (".",))?;

            let (module_name, class_name) = if !factory_path.contains('.') {
                let mut chars = factory_path.chars();
                let capitalized = match chars.next() {
                    None => String::new(),
                    Some(f) => f.to_uppercase().collect::<String>() + chars.as_str(),
                };
                (factory_path.to_string(), capitalized)
            } else {
                let parts: Vec<&str> = factory_path.rsplitn(2, '.').collect();
                let last_part = parts[0];
                let mut chars = last_part.chars();
                let first_char = chars.next();
                
                if let Some(c) = first_char {
                    if c.is_lowercase() {
                        let capitalized = c.to_uppercase().collect::<String>() + chars.as_str();
                        (factory_path.to_string(), capitalized)
                    } else {
                        (parts[1].to_string(), last_part.to_string())
                    }
                } else {
                    (parts[1].to_string(), last_part.to_string())
                }
            };

            ::log::debug!("Loading Python plugin module '{}', class '{}'", module_name, class_name);

            let module = match py.import(module_name.as_str()) {
                Ok(m) => m,
                Err(e) => {
                    let fallback = format!("sarracenia.flowcb.{}", module_name);
                    py.import(fallback.as_str()).map_err(|_| e)?
                }
            };

            let class = module.getattr(class_name.as_str())?;
            
            let py_config_code = r#"
import logging

class RustLogHandler(logging.Handler):
    def __init__(self, rust_log_fn):
        super().__init__()
        self.rust_log_fn = rust_log_fn
        self.setFormatter(logging.Formatter('%(message)s'))

    def emit(self, record):
        try:
            msg = self.format(record)
            self.rust_log_fn(record.levelno, record.name, msg)
        except Exception:
            self.handleError(record)

def setup_logging(rust_log_fn):
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = RustLogHandler(rust_log_fn)
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    logging.info("Python logging successfully routed to Rust log crate.")

class MockConfig:
    def __init__(self, kwargs, options_map):
        self._options_map = options_map
        for k, v in kwargs.items():
            setattr(self, k, v)
        if 'post_baseDir' not in kwargs:
            setattr(self, 'post_baseDir', None)

    def add_option(self, name, kind, default=None, *args):
        if name in self._options_map:
            val = self._options_map[name]
            logging.info("MockConfig: add_option found %s in options_map with value %s", name, val)
            if kind == 'list':
                val = [val]
            elif kind == 'set':
                val = set([x.strip() for x in val.split(',')])
            elif kind == 'flag':
                val = str(val).lower() in ('true', '1', 'yes')
            elif kind == 'count' or kind == 'int':
                val = int(val)
            setattr(self, name, val)
        else:
            logging.info("MockConfig: add_option did NOT find %s in options_map, using default %s", name, default)
            if not hasattr(self, name):
                setattr(self, name, default)
"#;
            let mock_module = PyModule::from_code(py, py_config_code, "mockconfig.py", "mockconfig")?;

            let rust_log_fn = pyo3::wrap_pyfunction!(rust_log, mock_module)?;
            mock_module.call_method1("setup_logging", (rust_log_fn,))?;

            let mock_class = mock_module.getattr("MockConfig")?;

            let kwargs = PyDict::new(py);
            kwargs.set_item("component", &_config.component)?;
            kwargs.set_item("config", _config.configname.clone().unwrap_or_else(|| "unknown".to_string()))?;
            kwargs.set_item("logLevel", &_config.log_level)?;
            if let Some(pbd) = &_config.post_base_dir {
                kwargs.set_item("post_baseDir", pbd.to_string_lossy().to_string())?;
            }

            let options_map = PyDict::new(py);
            for (k, v) in &_config.options {
                options_map.set_item(k, v)?;
            }

            let options = mock_class.call1((kwargs, options_map))?;
            
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
        if let Some(ack_id) = &msg.ack_id {
            dict.set_item("ack_id", ack_id)?;
        }
        
        for (k, v) in &msg.fields {
            dict.set_item(k, v)?;
        }
        for (k, v) in &msg.delete_on_post {
            dict.set_item(k, v)?;
        }
        Ok(dict)
    }

    fn py_to_msg(&self, dict: &PyDict) -> PyResult<Message> {
        let base_url: String = dict.get_item("baseUrl")?.unwrap().extract()?;
        let rel_path: String = dict.get_item("relPath")?.unwrap().extract()?;
        let mut msg = Message::new(&base_url, &rel_path);
        
        if let Ok(Some(ack_id)) = dict.get_item("ack_id").map(|i| i.map(|x| x.extract::<String>().unwrap_or_default())) {
            msg.ack_id = Some(ack_id);
        }
        
        for (k, v) in dict.iter() {
            let k_str: String = k.extract()?;
            if k_str != "baseUrl" && k_str != "relPath" && k_str != "ack_id" {
                if let Ok(v_str) = v.extract::<String>() {
                    if k_str.starts_with("new_") || k_str.starts_with("_") {
                        msg.delete_on_post.insert(k_str, v_str);
                    } else {
                        msg.fields.insert(k_str, v_str);
                    }
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

    async fn on_start(&mut self) -> anyhow::Result<()> {
        ::log::info!("PyWrapperPlugin::on_start called for {}", self.name);
        let instance_clone = self.instance.clone();
        let join_handle: tokio::task::JoinHandle<PyResult<()>> = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> PyResult<()> {
                if instance_clone.as_ref(py).hasattr("on_start")? {
                    instance_clone.call_method0(py, "on_start")?;
                } else {
                    ::log::info!("Python plugin does not have on_start method");
                }
                Ok(())
            })
        });
        join_handle.await.unwrap().map_err(|e| anyhow::anyhow!("Python plugin error in on_start: {:?}", e))?;
        Ok(())
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

    async fn on_housekeeping(&self) -> anyhow::Result<()> {
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
