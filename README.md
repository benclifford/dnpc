dnpc - Distributed Nested Performance Contexts
==============================================

This is a research prototype to understand how monitoring data from different
rich sources can be integrated to give a better understanding of how a
workflow-like application is executing.


Target applications:

1. lsst desc bps + parsl + Work Queue

To run the DESC specific processing code:

```
$ python -m dnpc.main
```


2. funcX: submit side + funcX hosted logs + endpoint logs

The interesting thing here compared to (1) is that the visibility of these
logs is different for different people: only an advanced investigator
will have access to all three of the above sets of logs, with the usual
situation being that funcX staff with only have access to the hosted logs
and end users access only either to their own logs and perhaps also the
endpoint logs - with no visibility inside the hosted services.

So the same runs can be presented as different views based on data access,
and understanding how that changes understanding is something interesting,
I think.
