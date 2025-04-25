# sparkles
Spark App Framework, reduces the need to have boilerplate and write a lot of foundation code just to create a Spark Data Pipeline project.

Planned milestones:
- Implement the generics to be able to create a job (done)
- Introduce file formats (done)
- Introduce HList for multiread (done)
- Introduce HList for multiwrite
- Introduce code formaters (scalafmt maybe?)
- Create Typed and Untyped interfaces - user can choose which to use.
- - - Typed will be a "cast" version of untyped under the hood but the user will not notice
- - Document usage of each of them
- Introduce file type inference
- Add read options (currently only file is there)
- - Create the code to be able to run jobs in memory, s3, etc.
- Implement the submission SDK for AWS
- Add support for Dataframes (json and dataframes are not good friends)
- Implement cluster monitoring and other orchestration needed EMR SDK 
