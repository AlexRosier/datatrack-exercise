# Datatrack exercise Alex

### Changes compared to assigement
- Running pyspark 3.5.0 still using a dataminded image
- Result of "Calculate the average of the measurements for a specific station by day" can be found under derived instead of clean

### Notes
- On "Calculate the average of the measurements for a specific station by day" I have partitioned by category_id as I think building dashboards per category is most logical and this would limit the number of files that need to be read. But other things are possible based on business requirements

### Open questions from my end
- When is a transform a transform ?
  - In the ingest folder you will see several "smashers" initially I flattened the json here but ultimately decided against it as seemed not in the spirit of the the exercise
  - What are badly performing operations in spark / how to identify those? (based on "signature")
  - 