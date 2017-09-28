## Morphium Drivers

the morphium project (look [here](http://sboesebeck.github.io/morphium/) for more details) was built as a way to access
mongodb in a easy to use fashion with a lot of features.

Sins a couple of months, morphium does support implementing your own `Drivers` do access any database. There are Drivers 
available, that use morphiums features for other databases like influxdb or redis.

This project is trying to implement an own driver for accessing mongodb in a better suited way than using the standard
mongodb implementation (which also sports some kind of data mapping already).