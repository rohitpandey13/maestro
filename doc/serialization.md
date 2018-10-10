Debugging and fixing serialization issues
=========================================

What causes serialization issues?
---------------------------------

Trying to serialize a value which can't be serialized. The only fix is
to figure out what values are being serialized, which values are failing,
and either stop serializing or fix the failing values.

What reasons have we seen Java serialization fail to serialize a value?

 - A Class does not implement the Serializable trait.
 - The Java serialization framework may try to load the class for a
   value, pick the wrong classloader, and fail to find the class.
 - Scala closures are fine as long as the variables they capture are fine.

What reasons have we seen Kryo serialization fail to serialize a value?

 - scalaz 7.1 has stopped producing generic signatures for it's classes,
   breaking Kryo's default serializer. Kryo's default serializer can't
   serialize classes with parameterized scalaz types as fields.


What values are being serialized? What values are failing to serialize?
-----------------------------------------------------------------------

The standard serialization errors aren't very helpful. To improve them:

 - Set the `CHILL_EXTERNALIZER_DEBUG` environment variable to `true` to get
   stack traces for Java serialization and Kryo.
 - Set the Java system property `sun.io.serialization.extendedDebugInfo` to
   `true` to get information about what classes and fields contain the class
   which is failing Java serialization.

Now errors should give you enough information to know what values are failing
to serialize.

You can look at the generated class files to find out how you are pulling
in these values. Try to map annonymous function names in the stack trace to
lambda functions in your source code. I used JD-GUI to decompile class files
as Java programs, which was convienient, albeit not without errors.

What reasons might you be pulling in values without necesarily realizing it?

 - Do you have inner classes pulling in the entire parent object?
 - Are your lambda classes pulling in more references than you expect?
   (A reference to `a.b.c` in your lambda will pull in `a`, not `c`)


How to fix serialization errors?
--------------------------------

Is it really necesary to serialize the failing values at all?

 - Lazy vals get turned into an `ObjectRef` and a `VolatileByteRef`, and a
   method to do the standard singleton pattern on those objects. These refs
   serialize just fine as long as the lazy val isn't calculated before serialization.
   Capturing the lazy value in a closure will capture the refs and serialize fine,
   but be careful: other innocent actions such as passing the lazy value to
   or from a method will force it's calculation and break serialization.

 - Instead of calculating a value to be captured by a lambda expression, consider
   manually creating a class extending `Function`, which constructs the values
   when the apply method is first called. This is less convienient but more
   explicit and less fragile than using lazy vals.

 - Instead of capturing the whole world in a closure by refering to `a.b.c`,
   assign `a.b.c` to a variable and capture the variable.

 - If you have an inner class pulling in the outer class, move it outside the parent
   class and explicitly pass it just the data it requires.

Can you fix the failing class?

 - Custom Kryo serializers can be defined for classes.
 - If Java serialization is broken there may be nothing you can do about it.
 - Stop using the non-serializable class. Use a serializable alterntive.
