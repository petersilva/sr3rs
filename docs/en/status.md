
# Status of the Project.

Tracking what is going on in the project.


* have a pas_sr3rs branch in sr_insects.


## testing method.
Using the insects/static_flow tests 

 * f00 - post t_dd_f* seems to work, 
      * they post to the sarra_download_f20 queue.  2250 messages show up, 
      * 1125 files in the right tree.
 * f20 - the sarra download works perfectly also -- leaving 1125 messages for subscribe_amqp
 * f30 - subscribe/amqp_f30 ...
      * 1125 files in the right tree. TESTROOT/downloaded_by_amqp
 * f40 - watch/f40 ... loses messages. 
 * f50 - sender/tsource2send send to TESTROOT/sent_by_tsource2send.
# Missing/deferred features.

* for Messages: only AMQP091 for now.  Want to get that fully working, then do the others. 
* for transfer: only ftp and sftp for now.  Get static flow passing first.
* sanity is missing.
* polls not tested yet, except for VIP failover.

-- build static flow test.
-- test poll.

-- sender path baseDir not correct.

-- audit: complete v02 and v03 messages... 
    -- checksums are currently missing ? 
    -- also is the format correct?



# Incompatibilities

## Stuff to fix later.

search path for python modules is currently hard coded to let the tests work.


## delete_on_post

In the python implementation, there is a field in messages called *deleteOnPost*  which contains the names of fields
that are to be deleted prior to publishing a message. In the rust implemetnation, it is different, there is a 
separate HashMap so we just omit the single delete_on_post HashMap from messages sent.  But that means that all temp 
stuff in messages is m.delete_on_post[ 'key' ] = ['value' ] ...   

If a python plugin sets such a field, it needs a mapping when calling it.


# Done!

Elements already addressed in the current implementation.

-- host state dir... forget what the option is called.
-- GUI change config root, to work with other trees.
-- delete fields present in posted messages.
-- declare declare declare get three different queues.
-- exchanges declared properly.
-- remove password from log messages that post urls.
-- HA failover. vip config setting.
-- --users declare
-- only accepts one pattern, rather than a whole bunch of options.
-- poll?
-- watch.
-- watch/f40  baseUrl file:// but pathis 
-- download mirroring tree.
-- default value of mirror is true in all components except subscribe.
-- NEW/STOPPED subscriptions.json created
-- post of a directory posts whole tree.
-- enable and disable actions.
-- edit edit action.
-- de-duplication in subscriptions.json.
-- refactor to reproduce moth class with mqtt v5 support
-- add GPLv2 license.
-- flowcb plugin architecture. but for now, no dynamic ones.

-- sr3rs view starts up a GUI with a canvas. On the canvas there are:
    * a rectangle for each flow. A flow is defined by a file under ~/.config/sr3rs/<component>/<configuration>.conf and the sr3rs status command shows one flow per line of output. 
    * there would be green arrows between rectangles where one flow publishing to a broker and exchange that the other is subscribing to.
    * each rectangle would have its name inside it.
    * the green arrows would be labelled with the consumer side's binding topics on that exchange.
    * when you click on a rectangle, a new dialog with the configuration in it should show up.  It could as simple as a graphical vim editing window.  

Dunno:

-- debug... don't have the per-module debugging logic. 

Notes:

now implemented both native Rust plugins and dynamic Python plugin loading in a highly efficient hybrid architecture!

How the Hybrid Architecture Works:
   1. The user specifies a plugin in a .conf file via flowCallback sarracenia.flowcb.log.Log.
   2. The Rust BaseFlow parses this config and asks flowcb::get_plugin() for the implementation.
   3. get_plugin first checks if "log" corresponds to any natively built Rust plugin (like log or sample). If it finds one, it returns the blazing-fast Rust
      implementation natively.
   4. If the plugin isn't recognized (e.g., flowCallback my_custom_plugin), get_plugin automatically falls back to creating a PyWrapperPlugin.
   5. The PyWrapperPlugin spins up the embedded Python interpreter (via PyO3), adds ~/.config/sr3/plugins to the sys.path, loads the .py file, and instantiates the
      Python class just like the original Python Sarracenia!
   6. When the Rust flow loop reaches an execution phase (like after_accept), it hands off its Rust Worklist to the PyWrapperPlugin. 
   7. The wrapper seamlessly serializes the Worklist and Messages into Python dicts, executes the python plugin off the async runtime thread via
      tokio::task::spawn_blocking (safely dodging the GIL), and maps the resulting arrays back to standard Rust structs!

This is the ultimate migration strategy—existing users with custom Sarracenia Python plugins won't have to rewrite a single line of code, but the underlying system
gets all the performance and safety benefits of Rust!


# Dreams:

Future enhancements.

## vip in all components, rather than just poll.

  * components in passive mode look at the output exchange, and prime their duplicate suppression cache that way.
  * components in passive mode do not perform their gather phase other than looking at the output exchange as just explained.
  * active modes perform the standard gather function for whatever the component does.
  * active mode publishes to the output_exchange.


## A stable ABI for rust plugins?

  * This is a not very idiomatic rust. I'm still trying to understand. It seems like rust avoids dynamic libraries,
    and wants everything to be compiled at once. The current plugin architecture means distributing as source
    and compiling the whole application with plugins at once. It's kind of the opposite of what a plugin should be.
