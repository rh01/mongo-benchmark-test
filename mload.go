package main

import (
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"log"
	"os"
	"runtime" //for goroutine
	"time"
	//"reflect" //for test
	"encoding/json"
	"io/ioutil"
	"math/rand"
)

var logfile *os.File
var logger *log.Logger
var datainfo *Datainfo
var jsondata = make(map[string]interface{})
var jsonMap = make(map[string]interface{})
var r *rand.Rand

type Mongobench struct {
	host            string
	port            string
	userName        string
	passWord        string
	dname           string
	cname           string
	cpunum          int
	datanum         int
	procnum         int
	geofield        string
	mongoClient     *mgo.Session
	mongoDatabase   *mgo.Database
	mongoCollection *mgo.Collection
}

type Datainfo struct {
	Name string
	Num  int
	Lng  float64
	Lat  float64
}

func GetMongoDBUrl(addr, userName, passWord string, port string) string {
	var mongoDBUrl string

	if port == "no" {
		if userName == "" || passWord == "" {
			mongoDBUrl = "mongodb://" + addr
		} else {
			mongoDBUrl = "mongodb://" + userName + ":" + passWord + "@" + addr
		}

	} else {
		if userName == "" || passWord == "" {
			mongoDBUrl = "mongodb://" + addr + ":" + port
		} else {
			mongoDBUrl = "mongodb://" + userName + ":" + passWord + "@" + addr + ":" + port
		}
	}
	return mongoDBUrl
}

func Newmongobench(host, userName, passWord, port, dbName, collectionName string, cpunum, datanum, procnum int, geofield string) *Mongobench {
	mongobench := &Mongobench{host, port, userName, passWord, dbName, collectionName, cpunum, datanum, procnum, geofield, nil, nil, nil}
	return mongobench
}

func ReadJson(filename string) (map[string]interface{}, error) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Println("ReadFile:", err.Error())
		return nil, err
	}
	if err := json.Unmarshal(bytes, &jsondata); err != nil {
		logger.Println("unmarshal:", err.Error())
		return nil, err
	}
	return jsondata, nil

}

func (mongobench *Mongobench) Conn(mongoDBUrl string) {
	var err error
	mongobench.mongoClient, err = mgo.Dial(mongoDBUrl)
	if err != nil {
		logger.Println("Connect to", mongobench.host, "UserName is ", mongobench.userName, "PassWord is", mongobench.passWord, "Failed")
	}
	logger.Println("Connect to", mongobench.host, "UserName is ", mongobench.userName, "PassWord is", mongobench.passWord, "Success")
	mongobench.mongoDatabase = mongobench.mongoClient.DB(mongobench.dname)
	mongobench.mongoCollection = mongobench.mongoDatabase.C(mongobench.cname)
	logger.Println("Use ", mongobench.dname, ".", mongobench.cname, mongobench)
}

func (mongobench *Mongobench) InsertData(jsoninfo string, ch chan int) {

	datainfo := Datainfo{"Edison", 1, 117.867589, 35.895416}
	for i := 0; i < mongobench.datanum; i++ {

		a := datainfo.Num * r.Intn(mongobench.datanum)
		lng := datainfo.Lng + float64(r.Intn(10))*r.Float64()
		lat := datainfo.Lat + float64(r.Intn(10))*r.Float64()
		if jsoninfo == "no" {
			//err := mongobench.mongoCollection.Insert(bson.M{"Name": datainfo.Name, "Num": a})
			loc := bson.M{"type": "Point", "coordinates": []float64{lng, lat}}
			err := mongobench.mongoCollection.Insert(bson.M{"Name": datainfo.Name, "Num": a, mongobench.geofield: loc})

			if err != nil {
				logger.Println("insert failed:", err)
				os.Exit(1)
			}
		} else if jsoninfo == "yes" {

			err := mongobench.mongoCollection.Insert(jsonMap)
			if err != nil {
				logger.Println("insert failed:", err)
				os.Exit(1)
			}
		}

	}
	ch <- 1

}

func (mongobench *Mongobench) QueryData(all bool, geo bool, ch chan int) {

	datainfo := Datainfo{"Edison", 1, 117.867589, 35.895416}
	if all == true {
		result1 := []int{}
		for i := 0; i < mongobench.datanum; i++ {
			var query bson.M
			if geo == false {
				b := datainfo.Num * r.Intn(mongobench.datanum)
				query = bson.M{"Num": b}
			} else {
				lng := datainfo.Lng + float64(r.Intn(10))*r.Float64()
				lat := datainfo.Lat + float64(r.Intn(10))*r.Float64()
				query = bson.M{mongobench.geofield: bson.M{
					"$nearSphare": bson.M{
						"$geometry": bson.M{
							"type":        "Point",
							"coordinates": []float64{lng, lat}},
						"$maxDistance": r.Intn(1000)}}}
			}
			//logger.Println("Query : ", query)
			mongobench.mongoCollection.Find(query).All(&result1)
			//logger.Println("Results findAll: ", result1)
		}

	} else {

		var result1 interface{}
		for i := 0; i < mongobench.datanum; i++ {
			var query bson.M
			if geo == false {
				b := datainfo.Num * r.Intn(mongobench.datanum)
				query = bson.M{"Num": b}
			} else {
				lng := datainfo.Lng + float64(r.Intn(10))*r.Float64()
				lat := datainfo.Lat + float64(r.Intn(10))*r.Float64()
				query = bson.M{mongobench.geofield: bson.M{
					"$nearSphare": bson.M{
						"$geometry": bson.M{
							"type":        "Point",
							"coordinates": []float64{lng, lat}},
						"$maxDistance": r.Intn(2000)}}}
			}
			//logger.Println("Query : ", query)
			mongobench.mongoCollection.Find(query).One(&result1)
			//logger.Println("Results findOne: ", result1)
		}
	}
	ch <- 1

}

func (mongobench *Mongobench) UpdateData(ch chan int) {
	datainfo := Datainfo{"Edison", 1, 117.867589, 35.895416}
	for i := 0; i < mongobench.datanum; i++ {
		b := datainfo.Num * r.Intn(mongobench.datanum)
		query := bson.M{"Num": b}
		update := bson.M{"$set": bson.M{"ii": []bson.M{{"i": 58506, "a": 1}}}}
		mongobench.mongoCollection.Update(query, update)
	}
	ch <- 1
}

func (mongobench *Mongobench) CleanJob() {
	logger.Println("Start clean database :mongobench")
	mongobench.mongoDatabase.DropDatabase()

}
func (mongobench *Mongobench) AddIndex() {
	logger.Println("Start build index Num")

	mongobench.mongoCollection.EnsureIndexKey("Num")
	index := mgo.Index{
		Key:  []string{"$2dsphere:" + mongobench.geofield},
		Bits: 26,
	}
	mongobench.mongoCollection.EnsureIndex(index)
}

func main() {
	var host, userName, passWord, port, logpath, jsonfile string
	var operation, geofield, collection, db string
	var queryall, clean, geo bool
	var cpunum, datanum, procnum int
	var err1 error
	var multi_logfile []io.Writer

	r = rand.New(rand.NewSource(time.Now().UnixNano()))

	flag.StringVar(&host, "host", "", "The mongodb host")
	flag.StringVar(&userName, "userName", "", "The mongodb username")
	flag.StringVar(&passWord, "passWord", "", "The mongodb password")
	flag.StringVar(&port, "port", "27017", "The mongodb port")
	flag.IntVar(&cpunum, "cpunum", 1, "The cpu number wanna use")
	flag.IntVar(&datanum, "datanum", 10000, "The data count per proc")
	flag.IntVar(&procnum, "procnum", 4, "The proc num ")
	flag.StringVar(&logpath, "logpath", "./log.log", "the log path ")
	flag.StringVar(&jsonfile, "jsonfile", "", "the json file u wanna insert(only one json )")
	flag.StringVar(&operation, "operation", "", "the operation ")
	flag.BoolVar(&queryall, "queryall", false, "query all or limit one")
	flag.BoolVar(&clean, "clean", false, "Drop the Database which --db given")
	flag.BoolVar(&geo, "geo", false, "operation type(if true: test geo operation)")
	flag.StringVar(&db, "db", "mongobench", "The mongodb db name for testing")
	flag.StringVar(&collection, "collection", "data_test", "The mongodb collection name for testing")
	flag.StringVar(&geofield, "geofield", "loc", "2d sphere field")
	flag.Parse()
	//flag.Usage()
	logfile, err1 = os.OpenFile(logpath, os.O_RDWR|os.O_CREATE, 0666)
	defer logfile.Close()
	if err1 != nil {
		fmt.Println(err1)
		os.Exit(-1)
	}
	multi_logfile = []io.Writer{
		logfile,
		os.Stdout,
	}
	logfiles := io.MultiWriter(multi_logfile...)
	logger = log.New(logfiles, "\r\n", log.Ldate|log.Ltime|log.Lshortfile)

	mongourl := GetMongoDBUrl(host, userName, passWord, port)

	if host != "" && operation != "" && clean == false {
		logger.Println("=====job start.=====")
		logger.Println("start init colletion")
		logger.Println(db, collection, geofield, geo, datanum, operation)
		mongobench := Newmongobench(host, userName, passWord, port, db, collection, cpunum, datanum, procnum, geofield)

		mongobench.Conn(mongourl)
		defer mongobench.mongoClient.Close()

		if jsonfile != "" {
			var err error
			jsonMap, err = ReadJson(jsonfile)
			logger.Println(jsonMap)
			if err != nil {
				logger.Println(err)
			}
		}

		if operation == "insert" {
			chs := make([]chan int, mongobench.procnum)
			runtime.GOMAXPROCS(mongobench.cpunum)
			for i := 0; i < mongobench.procnum; i++ {
				fmt.Println(i)

				chs[i] = make(chan int)

				if jsonfile == "" {
					go mongobench.InsertData("no", chs[i])
				} else {
					go mongobench.InsertData("yes", chs[i])
				}
			}

			for _, cha := range chs {
				<-cha

			}
		} else if operation == "prepare" {
			chs := make([]chan int, mongobench.procnum)
			runtime.GOMAXPROCS(mongobench.cpunum)
			for i := 0; i < mongobench.procnum; i++ {
				fmt.Println(i)

				chs[i] = make(chan int)

				go mongobench.InsertData("no", chs[i])

			}

			for _, cha := range chs {
				<-cha

			}
			mongobench.AddIndex()

		} else if operation == "query" {

			chs := make([]chan int, mongobench.procnum)
			runtime.GOMAXPROCS(mongobench.cpunum)
			for i := 0; i < mongobench.procnum; i++ {
				fmt.Println(i)

				chs[i] = make(chan int)

				go mongobench.QueryData(queryall, geo, chs[i])

			}

			for _, cha := range chs {
				<-cha

			}

		} else if operation == "update" {
			ch := make([]chan int, mongobench.procnum)
			runtime.GOMAXPROCS(mongobench.cpunum)
			for i := 0; i < mongobench.procnum; i++ {
				fmt.Println(i)

				ch[i] = make(chan int)

				go mongobench.UpdateData(ch[i])

			}

			for _, cha := range ch {
				<-cha

			}

		} else if operation == "tps" {
			//query
			chs := make([]chan int, mongobench.procnum)
			runtime.GOMAXPROCS(mongobench.cpunum)
			for i := 0; i < mongobench.procnum; i++ {
				fmt.Println(i)

				chs[i] = make(chan int)

				go mongobench.QueryData(queryall, geo, chs[i])

			}
			//insert
			chs1 := make([]chan int, mongobench.procnum)
			runtime.GOMAXPROCS(mongobench.cpunum)
			for i := 0; i < mongobench.procnum; i++ {
				fmt.Println(i)

				chs1[i] = make(chan int)

				go mongobench.InsertData("no", chs1[i])

			}
			for _, chb := range chs1 {
				<-chb

			}
			for _, cha := range chs {
				<-cha

			}

		} else {
			fmt.Println("Only support operation prepare/insert/query!")

		}

		logger.Println("=====Done.=====")
	} else if host != "" && clean == true {

		logger.Println("=====job start.=====")
		logger.Println("start init colletion")

		mongobench := Newmongobench(host, userName, passWord, port, db, collection, cpunum, datanum, procnum, geofield)

		mongobench.Conn(mongourl)
		defer mongobench.mongoClient.Close()
		mongobench.CleanJob()
	} else {
		fmt.Println("Please use -help to check the usage")
		fmt.Println("At least need host parameter")

	}

}
