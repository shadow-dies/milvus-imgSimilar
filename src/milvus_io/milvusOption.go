package milvusio

import (
	colorhistogram "MilvusTest/src/color_similar"
	"context"
	"fmt"
	"image"
	"log"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

const (
	similarScore = 10000000000 //相似度阈值
)

func LoadData(milvusClient client.Client, ctx context.Context) error {
	errLoad := milvusClient.LoadCollection(
		ctx,
		"imgColor",
		false,
	)
	return errLoad
}

func Search(milvusClient client.Client, ctx context.Context, img image.Image) bool {
	imgHash := colorhistogram.ImgToIconHash(img)
	sp, _ := entity.NewIndexIvfSQ8SearchParam(16)
	// start := time.Now()
	searchResult, errSearch := milvusClient.Search(
		ctx,
		"imgIcon",
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
	// timeSearch := time.Since(start)
	// fmt.Println("search time:", timeSearch)
	// fmt.Printf("%#v\n", searchResult)
	// fmt.Printf("\n")
	for _, sr := range searchResult {
		if sr.Scores[0] < 10000000000 {
			fmt.Println("similarImg：", sr.IDs)
			fmt.Println(int64(sr.Scores[0]))
			return true
		}
	}
	return false

}

func ColorSearch(milvusClient client.Client, ctx context.Context, imgHash []float32) (bool, string) {
	sp, _ := entity.NewIndexIvfSQ8SearchParam(16)
	// start := time.Now()
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
	fmt.Printf("%#v\n", searchResult)
	for _, sr := range searchResult {
		if sr.Scores[0] < similarScore {
			data := sr.IDs.FieldData().GetScalars().GetStringData().Data[0]
			fmt.Println("similarImg：", data)
			fmt.Println(int64(sr.Scores[0]))
			return true, data
		}
	}
	return false, ""

}

func Release(milvusClient client.Client, ctx context.Context) error {
	errRelease := milvusClient.ReleaseCollection(
		ctx,
		"imgIcon",
	)
	return errRelease
}
