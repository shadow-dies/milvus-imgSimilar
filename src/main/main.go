package main

import (
	colorhistogram "MilvusTest/src/color_similar"
	milvusio "MilvusTest/src/milvus_io"
	"context"
	"fmt"
	"log"
	"net/http"

	_ "MilvusTest/src/main/docs"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/disintegration/imaging"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

type msgImage struct {
	Biz     string `json:"biz"`
	Fileid  string `json:"fileid"`
	Scene   string `json:"scene"`
	ExtraId string `json:"extra_id"`
	Url     string `json:"url"`
}

type resMsg struct {
	IsOpen    bool    `json:"isopen"`    //图片是否正常打开
	IsSimilar bool    `json:"isSimilar"` //是否相似
	ID        string  `json:"id"`        //相似图片ID
	Score     float64 `json:"score"`     //相似度
}

type Milvus struct {
	Ctx    context.Context
	Client client.Client
}

// @Summary 查找相似图片
// @Produce json
// @Param biz body string true "业务方"
// @Param fileid body string true "主id"
// @Param scene body string false "场景"
// @Param extra_id body string false "子id"
// @Param url body string true "图片地址"
// @Success 200 {object} resMsg "响应结果"
// @Router /api/search [post]
func (client Milvus) search(c *gin.Context) {
	fmt.Println("msg")
	var msg msgImage
	var rMsg resMsg
	if err := c.ShouldBindJSON(&msg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fmt.Println(msg)

	Image, errOpen := imaging.Open(msg.Url)
	imgHash := colorhistogram.GenerateColorHistogramhHash(Image)
	if errOpen != nil || Image == nil {
		fmt.Println(msg.Url + " open error")
		rMsg.IsOpen = false
		c.JSON(http.StatusBadRequest, rMsg)
	}
	fmt.Println(msg.Url, "open succeed")
	rMsg.IsOpen = true
	res, SimilarID := milvusio.ColorSearch(client.Client, client.Ctx, imgHash)
	fmt.Println(res)

	rMsg.IsSimilar = res
	rMsg.ID = SimilarID
	if res == false {
		id := msg.Biz + "/" + msg.Fileid + "/" + msg.Scene + "/" + msg.ExtraId
		client.insert(id, imgHash)
	}
	c.JSON(http.StatusOK, rMsg)
}

func (client Milvus) insert(ID string, imgHash []float32) {
	imgIntros := make([][]float32, 0, 4096)
	imgNames := make([]string, 0, 4096)
	imgNames = append(imgNames, ID)
	imgIntros = append(imgIntros, imgHash)
	_, errInsert := client.Client.Insert(
		client.Ctx,
		"imgColor",
		"",
		entity.NewColumnVarChar("imgName", imgNames),
		entity.NewColumnFloatVector("imgHash", 4096, imgIntros),
	)
	if errInsert != nil {
		log.Fatal("failed to insert data:", errInsert.Error())
	}
	client.Client.Flush(client.Ctx, "imgIcon", true)
	fmt.Println(ID, "insert succeed")
}

func main() {
	ctx := context.Background()
	milvusClient, err1 := client.NewGrpcClient(
		ctx,               // ctx
		"localhost:19530", // addr
	)
	client := Milvus{Ctx: ctx, Client: milvusClient}
	if err1 != nil {
		log.Fatal("failed to connect to Milvus:", err1.Error())
	}
	fmt.Println("start")
	errLoad := milvusio.LoadData(milvusClient, ctx)
	if errLoad != nil {
		return
	}
	fmt.Println("loadsucceed")
	r := gin.New()
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	r.POST("/api/search", client.search)
	r.Run(":8080")

}
