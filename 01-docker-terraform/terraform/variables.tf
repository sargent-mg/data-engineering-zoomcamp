variable "credentials" {
  description = "My Google Credentials"
  type = string
  default = "./keys/my-creds.json"
  sensitive = true
}

variable "location" {
  description = "Project Location"
  type        = string
  default     = "US"
}

variable "project" {
  description = "Project Name"
  type        = string
  default     = "dtc-de-course-454704"
}

variable "region" {
  description = "Project Region"
  type        = string
  default     = "northamerica-south1"
}

variable "bq_dataset_id" {
  description = "My Bigquery Dataset id"
  type        = string
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  type        = string
  default     = "dtc-de-course-454704-demo-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
  type        = string
}