variable "extract_lambda" {
    type = string
    default = "extract_lambda"
}

variable "transform_lambda" {
    type = string
    default = "transform_lambda"
}

variable "load_lamabda" {
    type = string
    default = "load_lambda"
}

variable "extraction_utils" {
    type = string
    default = "extraction_utils"
}

variable "dependencies_zip_filename" {
    type = string
    # Manual workaround etag/source_hash error to force uploads to S3:
    default = "dependencies-1.zip"
}

variable "extraction_utils_zip_filename" {
    type = string
    # Manual workaround etag/source_hash error to force uploads to S3:
    default = "extraction_utils-2.zip"
}
