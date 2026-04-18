# Developer Notes.



## features ignored or deferred.

* windows compatibility. If we forget about it, we can use pathbufs everywhere without confusion.

* messages containing

* python search path for python plugins. It does not know where to look for sr3 built-in ones.



## Choices


### Using Pathbufs and Paths instead of Strings

When referring to hierarchical file system paths, the python implementation, in order to 
support windows (whose / is backwards), used strings instead of paths throughout. 
The Rust implementation started the same way, but there is a goal of moving all file paths 
to using std::path( Path, PathBuf ); over time. Not there yet.


### Improved path mangling Strategy

The Python implementation has a number of inscrutable routines:  updatePaths, updateFieldsAccepted,
and some initially straightforward strategies, that actually make things harder to understand.
For example, when you gather in python, it uses the post_base_url etc... directly.  This is confusing
because other components do it much later.  It is especially confusing with multiple publishers.

The Rust implementation is using the same path mangling strategy for all components,
and the *gather_file* output will always have a file:<something> base_url.  then it goes
through filtering, and work, and post... and in the post phase, ther post_ attributes get 
applied. So now there are two routines:

  * message_adjust_filter -- determines where the file will be written to: new_dir and new_file.

  * message_adjust_post -- applies post_ settings, per publisher.


## Incompatibilities



### DeleteOnPost


In the python implementation of message fields for internal application, the fields have no special names.
There is one additiona field: "_deleteOnPost" which is a set, containing the names of the internal fields.

In Rust, the message has instead a separate HashMap called delete_on_post.  And the fields are put 
in that HashMap, rather than the customary *fields* one.


so Python:
```python

msg['_deleteOnPost'] |= set([ 'new_dir' ] )
msg['new_dir'] = 'xyz'
 
```

in the Rust version would be:
```rust

msg.delete_on_post.insert("new_dir".to_string()), "xyz".to_string() );


```



