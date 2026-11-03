variable "region" {
  type    = string
  default = "us-east-1"
}

variable "profile" {
  description = "Perfil del AWS CLI; null usa el perfil default / variables de entorno"
  type        = string
  default     = null
}

variable "my_ip_cidr" {
  description = "Tu IP pública en /32 para abrir SSH (curl -s ifconfig.me)"
  type        = string
}

variable "public_key_path" {
  type    = string
  default = "~/.ssh/taller-dw.pub"
}

variable "ec2_type" {
  type    = string
  default = "t3.large"
}
