
# Malefico - A Python implementation  of a Mesos Scheduler/Executor driver
###### dangerous

The main goal is to provide a low-complexity and feature rich support for pure python Mesos Frameworks, but also to learn things.

## There be dragons
Malefico is quite experimental right now. It currently uses Tornado as the networking engine, but this might change in later release.  So the API might break.

This is mostly a learning project right now, but seems to work ok.

## Notable Features

-  Pure python so no C++ meddling
- Full featured Zookeeper and Redirect based Master detection
- Dict based for simplicity
- Task scheduling should be quite fast due to the asynchronous nature of the networking engine
- Fairly simple if somewhat magical(master detection) codebase

## Install

Not on pypi right now. Install from this repository.

Tested Python Versions:
- 2.7
- 3.5
- 3.6

Requirements:
- Mesos 0.28.0 and up
- Zookeeper

## Examples
Documentation is not properly written yet, but will be working on this soon.


## Tests
Also not there yet.

 
## Examples
An example Mesos Scheduler and Executor can be found in the examples folder.   It runs one task and then starts declining offers. The Task basically transmits and prints a message.

## Points to consider

 1. Extremely verbose non debug logging is used at the moment
 2. Some concerns about thread safety in a few places
 3. Using inline callbacks might be bad design, I am not a Tornado guy

## Outlook
The longterm goal is for this to serve as a base for Satyr and other more high level Python based frameworks.
This might go fully asynchronous based event handler. I am currently looking at a better Tornado, Twisted, Asyncio or Curio implementation.

## Acknowledgements
This has be heavily influenced by [Satyr](https://github.com/lensacom/satyr) and [PyMesos](https://github.com/douban/pymesos) and shares some utility code with both. 
The RecordIO format parsing was liftes from mrocklins [gist](https://gist.github.com/mrocklin/72cfd17a9f097e7880730d66cbde16a0)