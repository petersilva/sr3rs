# Sarracenia v3 (MetPX-Sr3rs)

Sr3 in Rust, using AI.
Purpose of this project is for me to learn Rust and get some experience with AI.

NOT READY FOR USE. INCOMPLETE, MAY EAT YOUR DOG. Changing hourly.

This is a proof of concept for now.

* For CLI, it copies the design and techniques of the python implementation.
   * replace *sr3 anything* by *sr3rs anything* and it should do the same thing.
   * it uses the same configs, but under ~/.config/sr3rs directory. Just copy and try them out.
   * state files are under ~/.cache/sr3rs.

* what is implemented: 
   * All components, rudimentarily.  testing with static flow for now.
     * post, subscribe and sarra seem to work fine.
     * watch loses a percentage of posts, so not really.
     * sender... I'm confused about it.  paths are wrong.
   * the messages themselves are all wrong... don't try any interop tests. they work with each other for now.
   * All of the configuration options that are implemented, should do the same thing they do in python.
   * options that have not yet had use cases, are not implemented yet.
   * it is kind of essentialist... things needed to run certain use cases are implemented, but many options/details missing.
   * it should be able to run python flowcb plugins... but not really tested yet.  *NOTE: search path is currently wrong*
   * Can write much quicker/faster/better rust plugins too.

* Rust version:
   * There is a GUI? really? there is!
   * There is a web ui too... it's the same as the GUI.
   * is completely async, heavily based on tokio.
   * was researched to be idiomatic rust, and use rust conventions.
     * internal vars do not use camel_case for example.
   * does directory tree walks properly 1 batch at a time... python walks the whole tree in one shot.
   * there is a beauutiful plugin architecture for rust plugins, but they need to be compiled in, so it´s kind of *built-in plugins*.
   * one of the built-in rust plugins is a plugin to run existing sr3 python flowcb plugins if it cannot find a rust one.
   * has little (no?) documentation yet.


## Getting Started


This should install compiled binaries in ~/.cargo/bin:

```shell

   % install_path=`pwd`
   % cargo install --path=${install_path}

```
The main entry point is *sr3rs*, but just like the python version, there is also *sr3rs_post.*



### Native Graphical UI

If you are on linux or perhaps WSL, the local ui might work. to  try it out:

```shell

   # git clone..
   cargo run view

```
That should start up the GUI, looking at the default location for the configuration tree: ~/.config/sr3rs.
When it's running on the default configuration tree, it also looks at ~/.cache/sr3rs, so you can do 
operations on configurations with the right click menu (edit|start|stop|enable|disable)


After that, entry points can be invoked in the shell with sr3rs_ui, or sr3rs view.

Many deployments store the configuration tree for a cluster in a git repo, and editing happens
when it isn't in the normal place (eg. ~/sr_config/ddsr ) so there is a  *configDir* option:

```

sr3rs_view --configDir=~/sr_config/ddsr


```
When using with a non-standard configDir, only editing function remains available.
Note that the configuration tree for the python and rust implementations are the same, so the
GUI can be used to browse python configuration trees as well. 

In the GUI, one can move the nodes around, and the state file is stored under the configDir,
so a persistent arrangement of the boxes is kept. 



### To run the Web UI

Note: there is no security on the web ui... so do this only where it is reasonably safe.

```shell

# clone the source repo.
trunk build
cargo run webui --port=8080 --configTree=~/.config/sr3rs

```
open a browser window for localhost:8080


## Documentation

look under docs.  There are en and fr sub-dirs for the languages.
Just a place holder for now.

[ sr3rs: [En](docs/en) [Fr](docs/fr) ]

only thing uptodate for now is hopefully: [status](docs/en/status.md) 

All of the following documentation refers to the python version (to be gradually replaced:):

## English

[ [homepage (En)](https://metpx.github.io/sarracenia) ] [ `[(Fr) fr/](<https://metpx.github.io/sarracenia/fr>) ]

[ [Getting Started](https://metpx.github.io/sarracenia/How2Guides/subscriber.html>) ] [ [Source Guide](https://metpx.github.io/sarracenia/How2Guides/source.html>) ] 


MetPX-sr3 (Sarracenia v3) is a data duplication
or distribution pump that leverages
existing standard technologies (web
servers and Message queueing protocol
brokers) to achieve real-time message delivery
and end-to-end transparency in file transfers.
Data sources establish a directory structure
which is carried through any number of
intervening pumps until they arrive at a
client.

## Français

(docs/fr) ] [ [Un bon départ](https://metpx.github.io/sarracenia/fr/CommentFaire/subscriber.html>) ] [ [Guide de Source](https://metpx.github.io/sarracenia/fr/CommentFaire/source.html>) ]

MetPX-sr3 (Sarracenia v3) est un engin de copie et de
distribution de données qui utilise des
technologies standards (tel que les services
web et le courtier de messages AMQP) afin
d'effectuer des transferts de données en
temps réel tout en permettant une transparence
de bout en bout. Alors que chaque commutateur
Sundew est unique en soit, offrant des
configurations sur mesure et permutations de
données multiples, Sarracenia cherche à
maintenir l'intégrité de la structure des
données, tel que proposée et organisée par la
source jusqu'à destination.
