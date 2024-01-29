package main

import (
	"github.com/abedmohammed/goDB/btree"
)

func main() {
	myTree := btree.NewC()
	myTree.Add("0", "my name is rawanannananannannnaa")
	myTree.PrintTree()
}
