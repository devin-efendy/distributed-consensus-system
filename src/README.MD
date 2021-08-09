# COMP3010 Assignment 3

# How to run the program:

`ssh` to one of the aviary machine. Assume we are connected to `hawk.cs.umanitoba.ca`

`cd` to the assignment directory

Run the following command to run the node:

```
$ ./service.py

OR

$ ./service.py 16000
```

To run the CLI run the following:

```
$ ./cli.py <hostname> 8023

in hawk:
$ ./cli.py hawk.cs.umanitoba.ca 8023
```

# To run the provided unit test : `test_server.py`

Change the `USE_LOCALHOST` variable in `service.py` (line 41) to `True`

Then run the following command:

```
$ ./service.py 16000

$ make -f makefile_a3.mk
```
