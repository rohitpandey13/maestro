maestro
=======

To build maestro, you need to add credentials for the commbank-releases artifactory
repository (whilst it depends on pre-open-sourced libraries).


To do this, add the following to your user sbt directory (i.e. `~/.sbt/<version>/commbank.sbt`):

```
credentials += Credentials("Artifactory Realm",
                           "commbank.artifactoryonline.com",
                           "<your artifactory username>",
                           "<your artifactory password>")
```
