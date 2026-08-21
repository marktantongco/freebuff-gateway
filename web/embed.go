package web

import "embed"

//go:embed *.html
//go:embed static/*
var FS embed.FS
