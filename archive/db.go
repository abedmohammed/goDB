package main

import (
	"fmt"
	"os"

	"github.com/abedmohammed/goDB/utils"
)

func main() {
	fmt.Println("Welcome to goDB")

	text := "Testing write to db"

	err := SaveData("testdb", []byte(text))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Completed!")
}

func SaveData(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, utils.RandomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	_, err = fp.Write(data)
	if err != nil {
		err = fp.Close()
		os.Remove(tmp)
		return err
	}

	err = fp.Sync() // fsync
	if err != nil {
		err = fp.Close()
		os.Remove(tmp)
		return err
	}

	err = fp.Close()
	if err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, path)
}
