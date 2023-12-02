resource "aws_batch_job_definition" "alex-datatrack" {
  name = "alex-datatrack-ingest"
  type = "container"
  platform_capabilities = ["EC2"]
  container_properties = jsonencode({
    command = ["python","ingest.py","--date","2023-11-29"],
    image   = "167698347898.dkr.ecr.eu-west-1.amazonaws.com/alex-datatrack:test"
    jobRoleArn = "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"
    executionRoleArn= "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"

    resourceRequirements = [
      {
        type  = "VCPU"
        value = "1"
      },
      {
        type  = "MEMORY"
        value = "2048"
      }
    ]

    environment = [
      {
        name  = "bucket"
        value = "data-track-integrated-exercise"
      }
    ]
  })
}

resource "aws_batch_job_definition" "alex-datatrack-clean" {
  name = "alex-datatrack-clean"
  type = "container"
  platform_capabilities = ["EC2"]
  container_properties = jsonencode({
    command = ["python3","spark_clean.py","--date","2023-11-29", "--bucket_path", "s3a://data-track-integrated-exercise/alex-data"],
    image   = "167698347898.dkr.ecr.eu-west-1.amazonaws.com/alex-datatrack-clean:test"
    jobRoleArn = "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"
    executionRoleArn= "arn:aws:iam::167698347898:role/integrated-exercise/integrated-exercise-batch-job-role"

    resourceRequirements = [
      {
        type  = "VCPU"
        value = "1"
      },
      {
        type  = "MEMORY"
        value = "2048"
      }
    ]

    environment = [
      {
        name  = "bucket"
        value = "data-track-integrated-exercise"
      }
    ]
  })
}