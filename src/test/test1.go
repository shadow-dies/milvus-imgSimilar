package test

import (
	colorhistogram "MilvusTest/src/color_similar"
	milvusio "MilvusTest/src/milvus_io"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/disintegration/imaging"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

func ColorhistogramBuildTest() {
	ctx := context.Background()
	milvusClient, err := client.NewGrpcClient(
		ctx,               // ctx
		"localhost:19530", // addr
	)
	if err != nil {
		log.Fatal("failed to connect to Milvus:", err.Error())
	}
	fmt.Println("create client succeed")
	defer milvusClient.Close()
	milvusClient.DropCollection(ctx, "imgColor")
	fmt.Println("drop collection succeed")
	var (
		collectionName = "imgColor"
	)
	schema := &entity.Schema{
		CollectionName: collectionName,
		Description:    "img search",
		Fields: []*entity.Field{
			{
				Name:        "imgName",
				DataType:    entity.FieldTypeVarChar,
				Description: "String",
				PrimaryKey:  true,
				AutoID:      false,
				TypeParams: map[string]string{
					"max_length": "50",
				},
			},
			{
				Name:        "imgHash",
				DataType:    entity.FieldTypeFloatVector,
				Description: "float vector",
				TypeParams: map[string]string{
					"dim": "4096",
				},
			},
		},
	}
	err = milvusClient.CreateCollection(
		ctx,
		schema,
		2,
	)
	if err != nil {
		log.Fatal("failed to create collection:", err.Error())
	}
	fmt.Println("create collection succeed")
	imgIntros := make([][]float32, 0, 4096)
	imgNames := make([]string, 0, 4096)
	files, _ := ioutil.ReadDir("./img")
	absPath, _ := os.Getwd()
	imgFile := "\\img\\"
	for _, f := range files {
		imgPath := absPath + imgFile + f.Name()
		Image, errOpen := imaging.Open(imgPath)
		if errOpen != nil || Image == nil {
			fmt.Println(imgPath + " open error")
			continue
		}
		fmt.Println(imgPath + " open succeed")
		imgNames = append(imgNames, f.Name())
		imgHash := colorhistogram.GenerateColorHistogramhHash(Image)
		// fmt.Println(imgHash)
		imgIntros = append(imgIntros, imgHash)

		if len(imgNames) >= 2048 {
			// fmt.Println(1)
			// nameColumn := entity.NewColumnVarChar("imgName", imgNames)
			// introColumn := entity.NewColumnFloatVector("imgHash", 363, imgIntros)
			_, errInsert := milvusClient.Insert(
				ctx,
				"imgColor",
				"",
				entity.NewColumnVarChar("imgName", imgNames),
				entity.NewColumnFloatVector("imgHash", 4096, imgIntros),
			)
			if errInsert != nil {
				log.Fatal("failed to insert data:", errInsert.Error())
			}
			imgNames = make([]string, 0, 4096)
			imgIntros = make([][]float32, 0, 4096)
		}
	}
	nameColumn := entity.NewColumnVarChar("imgName", imgNames)
	introColumn := entity.NewColumnFloatVector("imgHash", 4096, imgIntros)
	_, errInsert := milvusClient.Insert(
		ctx,
		"imgColor",
		"",
		nameColumn,
		introColumn,
	)
	if errInsert != nil {
		log.Fatal("failed to insert data:", errInsert.Error())
	}
	fmt.Println("insert succeed")

	idx, errIdx := entity.NewIndexIvfSQ8(
		entity.L2,
		16384,
	)
	if errIdx != nil {
		log.Fatal("fail to create ivf flat index parameter:", errIdx.Error())
	}
	fmt.Println("create indexParams succeed")
	errCraidx := milvusClient.CreateIndex(
		ctx,
		"imgColor",
		"imgHash",
		idx,
		false,
	)
	if errCraidx != nil {
		log.Fatal("fail to create index:", errCraidx.Error())
	}
	fmt.Println("create index succeed")
}

func ColorhistogramSearchTest() {
	abs_path := "E:\\GO\\src\\main"
	file1 := "\\8_gray.png"
	img1, _ := imaging.Open(abs_path + file1)
	colorhistogram.ImgToIconHash(img1)
	imgHash := colorhistogram.GenerateColorHistogramhHash(img1)

	ctx := context.Background()
	milvusClient, err := client.NewGrpcClient(
		ctx,               // ctx
		"localhost:19530", // addr
	)
	if err != nil {
		log.Fatal("failed to connect to Milvus:", err.Error())
	}

	errLoad := milvusClient.LoadCollection(
		ctx,
		"imgColor",
		false,
	)
	if errLoad != nil {
		log.Fatal("failed to load collection:", errLoad.Error())
	}
	sp, _ := entity.NewIndexIvfSQ8SearchParam(16)
	start := time.Now()
	searchResult, errSearch := milvusClient.Search(
		ctx,
		"imgColor",
		[]string{},
		"",
		[]string{"imgName"},
		[]entity.Vector{entity.FloatVector(imgHash)},
		"imgHash",
		entity.L2,
		1,
		sp,
	)
	if errSearch != nil {
		log.Fatal("fail to search collection:", errSearch.Error())
	}
	timeSearch := time.Since(start)
	fmt.Println("search time:", timeSearch)
	fmt.Printf("%#v\n", searchResult)
	fmt.Printf("\n")
	for _, sr := range searchResult {
		fmt.Println(sr.IDs)
		fmt.Println(int64(sr.Scores[0]))
	}

	errRelease := milvusClient.ReleaseCollection(
		ctx,
		"imgColor",
	)
	if errRelease != nil {
		log.Fatal("failed to release collection:", errRelease.Error())
	}
}

func TestIconBuild() {
	ctx := context.Background()
	milvusClient, err := client.NewGrpcClient(
		ctx,               // ctx
		"localhost:19530", // addr
	)
	if err != nil {
		log.Fatal("failed to connect to Milvus:", err.Error())
	}
	fmt.Println("create client succeed")
	defer milvusClient.Close()
	milvusClient.DropCollection(ctx, "imgIcon")
	fmt.Println("drop collection succeed")
	var (
		collectionName = "imgIcon"
	)
	schema := &entity.Schema{
		CollectionName: collectionName,
		Description:    "img search",
		Fields: []*entity.Field{
			{
				Name:        "imgName",
				DataType:    entity.FieldTypeVarChar,
				Description: "String",
				PrimaryKey:  true,
				AutoID:      false,
				TypeParams: map[string]string{
					"max_length": "50",
				},
			},
			{
				Name:        "imgHash",
				DataType:    entity.FieldTypeFloatVector,
				Description: "float vector",
				TypeParams: map[string]string{
					"dim": "363",
				},
			},
		},
	}
	err = milvusClient.CreateCollection(
		ctx,
		schema,
		2,
	)
	if err != nil {
		log.Fatal("failed to create collection:", err.Error())
	}
	fmt.Println("create collection succeed")
	imgIntros := make([][]float32, 0, 4096)
	imgNames := make([]string, 0, 4096)
	files, _ := ioutil.ReadDir("./img")
	absPath, _ := os.Getwd()
	imgFile := "\\img\\"
	for _, f := range files {
		imgPath := absPath + imgFile + f.Name()
		Image, errOpen := imaging.Open(imgPath)
		if errOpen != nil || Image == nil {
			fmt.Println(imgPath + " open error")
			continue
		}
		fmt.Println(imgPath + " open succeed")
		imgNames = append(imgNames, f.Name())
		imgHash := colorhistogram.ImgToIconHash(Image)
		imgIntros = append(imgIntros, imgHash)
		// fmt.Println(len(imgNames))
		if len(imgNames) >= 4096 {
			// fmt.Println(1)
			// nameColumn := entity.NewColumnVarChar("imgName", imgNames)
			// introColumn := entity.NewColumnFloatVector("imgHash", 363, imgIntros)
			_, errInsert := milvusClient.Insert(
				ctx,
				"imgIcon",
				"",
				entity.NewColumnVarChar("imgName", imgNames),
				entity.NewColumnFloatVector("imgHash", 363, imgIntros),
			)
			if errInsert != nil {
				log.Fatal("failed to insert data:", errInsert.Error())
			}
			imgNames = make([]string, 0, 4096)
			imgIntros = make([][]float32, 0, 4096)
		}
	}
	nameColumn := entity.NewColumnVarChar("imgName", imgNames)
	introColumn := entity.NewColumnFloatVector("imgHash", 363, imgIntros)
	_, errInsert := milvusClient.Insert(
		ctx,
		"imgIcon",
		"",
		nameColumn,
		introColumn,
	)
	if errInsert != nil {
		log.Fatal("failed to insert data:", errInsert.Error())
	}
	fmt.Println("insert succeed")

	idx, errIdx := entity.NewIndexIvfSQ8(
		entity.L2,
		16384,
	)
	if errIdx != nil {
		log.Fatal("fail to create ivf flat index parameter:", errIdx.Error())
	}
	fmt.Println("create indexParams succeed")
	errCraidx := milvusClient.CreateIndex(
		ctx,
		"imgIcon",
		"imgHash",
		idx,
		false,
	)
	if errCraidx != nil {
		log.Fatal("fail to create index:", errCraidx.Error())
	}
	fmt.Println("create index succeed")
}

func TestIconSearch() {

	abs_path := "D:\\Image\\flickr30k_images\\flickr30k_images"
	file1 := "\\2705101261.jpg"
	img1, _ := imaging.Open(abs_path + file1)
	colorhistogram.ImgToIconHash(img1)
	imgHash := colorhistogram.ImgToIconHash(img1)

	ctx := context.Background()
	milvusClient, err := client.NewGrpcClient(
		ctx,               // ctx
		"localhost:19530", // addr
	)
	if err != nil {
		log.Fatal("failed to connect to Milvus:", err.Error())
	}

	errLoad := milvusClient.LoadCollection(
		ctx,
		"imgIcon",
		false,
	)
	if errLoad != nil {
		log.Fatal("failed to load collection:", errLoad.Error())
	}
	sp, _ := entity.NewIndexIvfSQ8SearchParam(16)
	start := time.Now()
	searchResult, errSearch := milvusClient.Search(
		ctx,
		"imgIcon",
		[]string{},
		"",
		[]string{"imgName"},
		[]entity.Vector{entity.FloatVector(imgHash)},
		"imgHash",
		entity.L2,
		2,
		sp,
	)
	if errSearch != nil {
		log.Fatal("fail to search collection:", errSearch.Error())
	}
	timeSearch := time.Since(start)
	fmt.Println("search time:", timeSearch)
	fmt.Printf("%#v\n", searchResult)
	fmt.Printf("\n")
	for _, sr := range searchResult {
		fmt.Println(sr.IDs)
		fmt.Println(int64(sr.Scores[0]))
	}

	errRelease := milvusClient.ReleaseCollection(
		ctx,
		"imgIcon",
	)
	if errRelease != nil {
		log.Fatal("failed to release collection:", errRelease.Error())
	}
}

func TestInsert() {
	ctx := context.Background()
	milvusClient, err := client.NewGrpcClient(
		ctx,               // ctx
		"localhost:19530", // addr
	)
	if err != nil {
		log.Fatal("failed to connect to Milvus:", err.Error())
	}
	errLoad := milvusio.LoadData(milvusClient, ctx)
	if errLoad != nil {
		return
	}
	fmt.Println("load succeed")
	absPath := "D:\\Image\\flickr30k_images\\flickr30k_images"
	files, _ := ioutil.ReadDir(absPath)
	start := time.Now()
	for _, f := range files {
		fmt.Printf("\n")
		imgPath := absPath + "\\" + f.Name()
		Image, errOpen := imaging.Open(imgPath)
		if errOpen != nil || Image == nil {
			fmt.Println(imgPath + " open error")
			continue
		}
		fmt.Println(f.Name(), "open succeed")
		res := milvusio.Search(milvusClient, ctx, Image)
		if res == true {
			fmt.Println("insertImage: ", f.Name())
			print("\n")
		} else {
			fmt.Println(f.Name(), "start insert")
			imgIntros := make([][]float32, 0, 4096)
			imgNames := make([]string, 0, 4096)
			imgNames = append(imgNames, f.Name())
			imgHash := colorhistogram.ImgToIconHash(Image)
			imgIntros = append(imgIntros, imgHash)
			_, errInsert := milvusClient.Insert(
				ctx,
				"imgIcon",
				"",
				entity.NewColumnVarChar("imgName", imgNames),
				entity.NewColumnFloatVector("imgHash", 363, imgIntros),
			)
			if errInsert != nil {
				log.Fatal("failed to insert data:", errInsert.Error())
			}
			milvusClient.Flush(ctx, "imgIcon", true)
			fmt.Println(f.Name(), "insert succeed")
		}
		fmt.Printf("\n")
	}
	timeSearch := time.Since(start)
	fmt.Println("insert time:", timeSearch)
}
