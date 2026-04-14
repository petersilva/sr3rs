# Sarracenia v3 (MetPX-Sr3rs)

Sr3 in Rust.

NOT READY FOR USE. INCOMPLETE, MAY EAT YOUR DOG. Changing hourly.

This is a proof of concept for now.


* For CLI, it copies the design and techniques of the python implementation.
   * replace *sr3 anything* by *sr3rs anything* and it should do the same thing.
   * it uses the same configs, but under ~/.config/sr3rs directory. Just copy and try them out.
   * state files are under ~/.cache/sr3rs.

* what is implemented: 
   * All components, rudimentarily
   * the messages themselves are all wrong... don't try any interop tests. they work with each other for now.
   * All of the configuration options that are implemented, should do the same thing they do in python.
   * options that have not yet had use cases, are not implemented yet.
   * it is kind of essentialist... things needed to run certain use cases are implemented, but many options/details missing.
   * it should be able to run python flowcb plugins... but not really tested yet.  
   * Can write much quicker/faster/better rust plugins too.

* Rust version:
   * is completely async, heavily based on tokio. 
   * was researched to be idiomatic rust, and use rust conventions.
     * internal vars do not use camel_case for example.
   * does directory tree walks properly 1 batch at a time... python walks the whole tree in one shot.
   * there is a beauutiful plugin architecture for rust plugins, but they need to be compiled in, so it´s kind of *built-in plugins*.
   * one of the built-in rust plugins is a plugin to run existing sr3 python flowcb plugins if it cannot find a rust one.
   * has little (no?) documentation yet.

Development Method
------------------

This is a re-implementation of the python package, but in Rust, coded by guided use of Gemini AI,
because that´s what the cool kids are doing these days. I wanted to learn rust, and play with AI... and
this felt natural.

It is mostly dialog where I:

 * ask the Gemini/CLI to port some small part of functionality from the python package to the rust one.
 * It tries.
 * It tell it what is wrong with the result, either pointing it to unit tests or some other
   concrete behaviour.
 * It tries again.
 * Once it has some particular behaviour right, and the unit tests it created all pass, then I commit.
 * Rinse, Lather, Repeat.

Also, I have conceptual design discussions with ChatGPT. ChatGPT is the AI that made initial outlines
of the rust plugin architecture. Fed the resulting design documents to Gemini for implementation. That 
worked well.

Some incorrect behaviours, I can correct myself, but with only a rudimentary grasp of rust, not very many.
The development method is to build each component using very small configurations, then gradually
expand until a minimum viable component is available, then try the next component.  Working up
to flow tests.  The intent is to get to flow tests, and at least run them correctly.
The log files will be different though, so extensive massage of the flow tests will be needed
for them to correctly report.

Once in a while, I do audits, and correct code with a number of manual changes. For example, the original
python unit tests for messages used incorrect ones, and confused the AI's with contradictory messages,
so re-worked v02 and v03 postformat manually.


All of the following documentation refers to the python version (to be gradually replaced:):

[ homepage (En): https://metpx.github.io/sarracenia ] [ `(Fr) fr/ <https://metpx.github.io/sarracenia/fr>`_ ]

+----------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
|                                                                                        |                                                                                           |
| [ `Getting Started <https://metpx.github.io/sarracenia/How2Guides/subscriber.html>`_ ] | [ `Un bon départ <https://metpx.github.io/sarracenia/fr/CommentFaire/subscriber.html>`_ ] |
| [ `Source Guide <https://metpx.github.io/sarracenia/How2Guides/source.html>`_ ]        | [ `Guide de Source <https://metpx.github.io/sarracenia/fr/CommentFaire/source.html>`_ ]   |
|                                                                                        |                                                                                           |
+----------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
|                                                                                        |                                                                                           |
| MetPX-sr3 (Sarracenia v3) is a data duplication                                        | MetPX-sr3 (Sarracenia v3) est un engin de copie et de                                     |
| or distribution pump that leverages                                                    | distribution de données qui utilise des                                                   |
| existing standard technologies (web                                                    | technologies standards (tel que les services                                              |
| servers and Message queueing protocol                                                  | web et le courtier de messages AMQP) afin                                                 |
| brokers) to achieve real-time message delivery                                         | d'effectuer des transferts de données en                                                  |
| and end-to-end transparency in file transfers.                                         | temps réel tout en permettant une transparence                                            |
| Data sources establish a directory structure                                           | de bout en bout. Alors que chaque commutateur                                             |
| which is carried through any number of                                                 | Sundew est unique en soit, offrant des                                                    |
| intervening pumps until they arrive at a                                               | configurations sur mesure et permutations de                                              |
| client.                                                                                | données multiples, Sarracenia cherche à                                                   |
|                                                                                        | maintenir l'intégrité de la structure des                                                 |
|                                                                                        | données, tel que proposée et organisée par la                                             |
|                                                                                        | source jusqu'à destination.                                                               |
|                                                                                        |                                                                                           |
+----------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
