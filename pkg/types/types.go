package types

type Args struct {
	Config   string `arg:"required" help:"配置文件路径 (TOML格式)"`
	LogLevel string `arg:"--log-level" default:"info" help:"日志级别: debug, info, warn, error"`
	Version  bool   `arg:"--version" help:"显示版本信息"`
}

const Version = "0.1.0"
