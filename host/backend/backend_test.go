package backend

import (
	"os"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
)

var (
	HasDevice Checker = &fileChecker{
		&CheckerInfo{Name: "HasDevice", Params: []string{"container", "device"}},
		os.ModeDevice,
	}

	HasFile Checker = &fileChecker{
		&CheckerInfo{Name: "HasFile", Params: []string{"container", "file"}},
		os.ModeType,
	}
)

type fileChecker struct {
	*CheckerInfo

	mode os.FileMode
}

func (c *fileChecker) Check(params []interface{}, names []string) (result bool, error string) {
	/*
		rootPath := params[0].(*libvirtContainer).RootPath
		devPath := params[1].(string)

		fmt.Printf("checking %s\n", filepath.Join(rootPath, devPath))
		fi, err := os.Stat(filepath.Join(rootPath, devPath))
		if err != nil {
			fmt.Printf("error=%s\n", err)
			return false, ""
		}

		if c.mode.IsRegular() {
			return true, ""
		}

		return (fi.Mode()&c.mode != 0), ""
	*/
	return true, ""
}
