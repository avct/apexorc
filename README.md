# apexorc
Output to ORC files from github.com/apex/log 

## Overview

This package provides a handler for github.com/apex/log that will cause ```Entry``` structs to be persisted to an ORC file.  The following columns are present in the resulting ORC file:

  * timestamp ```timestamp```
  * level ```string```
  * message ```string```
  * fields ```map<string,string>```
  
Note that the map type in fields only supports strings so you will have to make a string representation of any data you want to store their.  Note also that using apex's ```.WithError``` function is actually just a shortcut to creating a field called ```error```, which is where you'll find any errors you use.

Additionally, a ```RotatingHandler`` is provided to allow for ORC log files to be rotated on demand.  No scheduling or other mechanism is provided, only the infrastructure for log rotation itself.  A typical strategy in UNIX like environments is to do rotation in response to a signal.

## Examples

### Simple logging to an ORC file:

```go
package main

import (
    "errors"

    "github.com/apex/log"
    "github.com/avct/apexorc"
)

func main() {
    handler := NewHandler("mylog.orc")
    // It's important to close the handler when we're done!
    defer handler.Close()
    
    log.SetHandler(handler)
    
    err := errors.New("Ouch")
    log.WithError(err).Error("An Orc attacked")
}
```

### Using a rotating handler with the provided numeric archiving algorithm

```go
package main

import (
    "errors"

    "github.com/apex/log"
    "github.com/avct/apexorc"
)

func main() {
    handler := apexorc.NewRotatingHandler("mylog.orc", apexorc.NumericArchiveF)
    // It's important to close the handler when we're done! As we currently don't support appending to an ORC file, we treat exiting a program as a reason to rotate.
    defer handler.Rotate()
    
    log.SetHandler(handler)
    
    err := errors.New("Ouch")
    log.WithError(err).Error("An Orc attacked")
    
    // The existing log file will be rotated to "mylog.orc.1"
    handler.Rotate()
    
    log.Info("This will get logged to a brand new mylog.orc")
    
    // When the program exits, the deferred Rotate() will move mylog.orc to mylog.orc.1 and the previous mylog.orc.1 will be moved to mylog.orc.2.  
}
```
