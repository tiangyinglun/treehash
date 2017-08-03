package main

import (
	"flag"
	"encoding/json"
	"fmt"
	"os"
	"time"
	"path/filepath"
	"strconv"
	"crypto/sha1"
	"regexp"
	"bytes"
	"io"
	"math"
	"io/ioutil"
	"runtime"
)

const (
	Success = iota

	PathNullErr

	ParamNo

	ParamError
)

const process = 100

var Message = map[int]string{
	Success:     "执行成功",
	PathNullErr: "根目录不存在",
	ParamNo:     "没有参数",
	ParamError:  "参数错误",
}

var BackMessage map[string]interface{}

func main() {
	startTime := currentTimeMillis()
	root := flag.String("root", "", "hash 树的根目录")
	filter := flag.String("filter", "", "过滤条件正则格式")
	model := flag.String("model", "", "选择模式fast|big")
	flag.Parse()
	args := flag.Args()

	for _, v := range args {
		if v == "help" {
			showHelp()
			os.Exit(1)
		}
	}

	if *root == "" {
		fmt.Println("")
		fmt.Println("  --没有填写hash的根目录")
		showHelp()
		os.Exit(1)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())
	if *model == "fast" || *model == "" {
		err := handlePathv1(*root, *filter)
		if err != nil {
			fmt.Println(err)
		}
	} else if *model == "big" {
		err := handlePath(*root, *filter)
		if err != nil {
			fmt.Println(err)
		}
	} else {
		showHelp()
		os.Exit(1)
	}

	fmt.Printf("执行完成共用了%dms", currentTimeMillis()-startTime)
}

func handlePathv1(root, filter string) error {
	var treeFile []string
	if !Exist(root) {
		fmt.Println(backMessage(PathNullErr))
		os.Exit(1)
	}

	var re *regexp.Regexp
	if filter != "" {
		re, _ = regexp.Compile(filter)
	}

	//遍历根文件下所有目录的文件
	filepath.Walk(root, func(root string, f os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if f.IsDir() {
			return nil
		}

		if filter != "" {
			b := re.MatchString(root)
			if !b {
				treeFile = append(treeFile, root)
			}
		} else {
			treeFile = append(treeFile, root)
		}

		return nil
	})

	if len(treeFile) < 1 {
		return nil
	}

	err := handleFileDatav1(treeFile)

	return err
}

func handleFileDatav1(data []string) error {

	count := len(data)
	c := make(chan int, process)
	f, err := os.OpenFile("E:/gows/src/test/gofile/demo1.txt", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		fmt.Println(err.Error())
	}

	defer f.Close()

	if count < 1000 {
		go writeToFile(data, c, f)
		<-c
		return nil
	}

	per := perArr(count)

	for k, v := range per {
		var slice []string
		if k == 0 {
			slice = data[0:v]
		} else {
			slice = data[per[k-1]:v]
		}
		go writeToFilev1(slice, c, f)
	}

	for i := 0; i < len(per); i++ {
		<-c
	}

	return nil
}

func writeToFilev1(data []string, c chan int, f *os.File) {

	var buffer bytes.Buffer
	for _, v := range data {
		targetStr := targetStrv1(v)
		buffer.WriteString(targetStr)
	}
	_, err := io.WriteString(f, buffer.String())
	if err != nil {
		fmt.Println(err.Error())
	}
	buffer.Reset()
	c <- 1
}

func targetStrv1(v string) string {
	fe, err := os.Open(v)
	defer fe.Close()
	if err != nil {
		fmt.Println(err.Error())
	}

	bt, err := ioutil.ReadAll(fe)
	if err != nil {
		fmt.Println(err)
	}
	fileInfo, err := fe.Stat()
	if err != nil {
		fmt.Println(err.Error())
	}
	return v + "," + sha1fun(string(bt)) + "," + strconv.FormatInt(fileInfo.Size(), 10) + "\n"
}

/***********************v-0**********************************/
/**
处理数据
 */

func handlePath(root, filter string) error {
	var treeFile []string
	if !Exist(root) {
		fmt.Println(backMessage(PathNullErr))
		os.Exit(1)
	}

	var re *regexp.Regexp
	if filter != "" {
		re, _ = regexp.Compile(filter)
	}

	//遍历根文件下所有目录的文件
	filepath.Walk(root, func(root string, f os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if f.IsDir() {
			return nil
		}

		if filter != "" {
			b := re.MatchString(root)
			if b {
				treeFile = append(treeFile, targetStr(root, f.Size()))
			}
		} else {
			treeFile = append(treeFile, targetStr(root, f.Size()))
		}

		return nil
	})

	if len(treeFile) < 1 {
		return nil
	}

	err := handleFileData(treeFile)

	return err
}

func handleFileData(data []string) error {
	count := len(data)
	c := make(chan int, process)
	f, err := os.OpenFile("E:/gows/src/test/gofile/demo.txt", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		fmt.Println(err.Error())
	}

	defer f.Close()

	if count < 1000 {
		go writeToFile(data, c, f)
		<-c
		return nil
	}

	per := perArr(count)

	for k, v := range per {
		var slice []string
		if k == 0 {
			slice = data[0:v]
		} else {
			slice = data[per[k-1]:v]
		}
		go writeToFile(slice, c, f)
	}

	for i := 0; i < len(per); i++ {
		<-c
	}

	return nil
}

func writeToFile(data []string, c chan int, f *os.File) {
	var buffer bytes.Buffer
	for _, v := range data {
		buffer.WriteString(v)
	}
	_, err := io.WriteString(f, buffer.String())
	if err != nil {
		fmt.Println(err.Error())
	}
	buffer.Reset()
	c <- 1
}

func perArr(count int) []int {
	fcount := float64(count)
	fprocess := float64(process)
	num := int(math.Ceil(fcount / fprocess))
	slice := make([]int, num)
	for i := 0; i < num; i++ {
		if i == num-1 {
			slice[i] = count
		} else {
			slice[i] = process + i*process
		}
	}
	return slice
}

/**
目标字符串
 */
func targetStr(name string, size int64) string {
	//fmt.Println(name)
	s, err := ioutil.ReadFile(name)
	if err != nil {
		fmt.Println(err)
	}
	nameStr := string(s)
	return name + "," + sha1fun(nameStr) + "," + strconv.FormatInt(size, 10) + "\n"
}

/**
 sha1 加密
 */
func sha1fun(str string) string {
	byteStr := []byte(str)
	return fmt.Sprintf("%x", sha1.Sum(byteStr))
}

/**
验证文件是否存在
 */
func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

/**
帮助函数
 */
func showHelp() {
	fmt.Println("")
	fmt.Println("  ******************************************")
	fmt.Println("")
	fmt.Println("  *  root hash 的根目录必填                 *")
	fmt.Println("")
	fmt.Println("  *  help 帮助函数                         *")
	fmt.Println("")
	fmt.Println("  *  filter  过滤条件正则格式              *")
	fmt.Println("")
	fmt.Println("  ******************************************")
	fmt.Println("")
}

func backMessage(code int) string {
	BackMessage = make(map[string]interface{})
	BackMessage["status"] = code
	BackMessage["message"] = Message[code]
	BackMessage["detail"] = ""
	js, err := json.Marshal(BackMessage)
	if err != nil {
		fmt.Println(err.Error())
	}
	return string(js)
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}
