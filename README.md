# fury-spark
A spark serializer implementation based on Apache/Fury.

# usage
set `spark.serializer` = `org.apache.spark.FurySerializer`

If you want to register classes, set `spark.fury.classesToRegister` like `org.example.A,org.example.B`.

The fury-spark supports custom serializer. You can add your custom serializer by implementing the `FuryRegistrator` trait and set the `spark.fury.registrator` like 'org.example.regA,org.example.regB'.
