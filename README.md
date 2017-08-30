# apexorc
Output to ORC files from github.com/apex/log 

## Overview

This package provides a handler for github.com/apex/log that will cause ```Entry``` structs to be persisted to an ORC file.  The following columns are present in the resulting ORC file:

  * timestamp ```timestamp```
  * level ```string```
  * message ```string```
  * fields ```map<string,string>```
  
Note that the map type in fields only supports strings so you will have to make a string representation of any data you want to store their.  Note also that using apex's ```.WithError``` function is actually just a shortcut to creating a field called ```error```, which is where you'll find any errors you use.

## Example

In order to log to an ORC file:

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


