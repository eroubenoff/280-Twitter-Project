# =========================================
# 0_authorization.R
# Sets up authorization 
# Ethan Roubenoff 4/19
# =========================================
library(streamR)
library(ROAuth)

requestURL <- "https://api.twitter.com/oauth/request_token"
accessURL <- "https://api.twitter.com/oauth/access_token"
authURL <- "https://api.twitter.com/oauth/authorize"
consumer_key <- "uVYqJJOGLU3Uiiky1M5HMN6jB"
consumer_secret <- "FCOZHOes4NLj686XZBT6ls8xLT31kt1oG6e8ipNdueKy2i5rw7"
access_token <- "3002599774-bTEuqbKtzEEPBa4sSM3Z0Mg2EygrirLvY4gXmva"
access_secret <- "bgKyirOwkqmfRunVbl9WLe9X0dxbONmJA7AcxDuIJrVRF"
oauthtoken <- list(consumer_key = consumer_key, consumer_secret = consumer_secret, access_token =  access_token, access_token_secret =  access_secret)

setup_twitter_oauth(consumer_key = consumer_key, consumer_secret = consumer_secret, access_token =  access_token, access_secret =  access_secret)
