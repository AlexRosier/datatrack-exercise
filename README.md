# Datatrack exercise Alex

### Changes compared to assigement
- Running pyspark 3.5.0 still using a dataminded image
- Result of "Calculate the average of the measurements for a specific station by day" can be found under derived instead of clean
- I have build a codepipeline and the ECR repository using cloudformation as I already had a cloudformation template that works. Batch job definitions are still created using terraform.

### Notes
- "STATIONS_AVERAGE_VALUE" table is build so that each row is a station for a category and the average of that category measurements for that day. I think this allows for greater query flexibility.
- On "Calculate the average of the measurements for a specific station by day" I have partitioned by category_id as I think building dashboards per category is most logical and this would limit the number of files that need to be read. But other things are possible based on business requirements
- For pollution_per_city_and_category I have used the "station_native_city" field which is derived from the station label field. I use this as this how the original data "logic" works and geopy results are not great.
- For stations_per_city I do use geopy data as that is what is suggested in the exercise. Though I think the results are subpar. To improve this geopy could still be used but by using a paying provider at its backend.
- I append the data in snowflake rather than overwrite. This allows for querying over a longer timeframe. Should a job get rerun the data of that day will first be deleted.

### Approach
- I have approached this project as what it was a training exercise as such I have focussed a lot on things that where new to me.

### Open questions from my end
- When is a transform a transform ?
  - In the ingest folder you will see several "smashers" initially I flattened the json here but ultimately decided against it as seemed not in the spirit of the the exercise. And it allow me to learn more about spark rather than python.
 - What are badly performing operations in spark / how to identify those? (based on "signature")
 - Any and all feedback is welcome :) 