# Architecture

Documentation of the sr3rs architecture.


## Development Method

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


