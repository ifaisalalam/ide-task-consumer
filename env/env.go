package env

import "github.com/spf13/viper"

const (
	Local      = "local"
	Production = "production"
	Staging    = "staging"
	Testing    = "test"
)

var AppEnv = Local

func InitEnv() {
	AppEnv = viper.GetString("ENV")
	if AppEnv == "" {
		AppEnv = Local
	}
}
