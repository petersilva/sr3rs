# Developer Notes.



## features ignored or deferred.

* windows compatibility. If we forget about it, we can use pathbufs everywhere without confusion.

* messages containing

* python search path for python plugins. It does not know where to look for sr3 built-in ones.





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



