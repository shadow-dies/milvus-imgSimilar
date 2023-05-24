package colorhistogram

import (
	"image"

	"github.com/vitali-fedulov/images4"
)

// func Cosine_similarity(arr1, arr2 []float32) float64 {
// 	total, total_a, total_b := 0, 0, 0

// 	for i := 0; i < 4096; i++ {
// 		total += arr1[i] * arr2[i]
// 		total_a += arr1[i] * arr1[i]
// 		total_b += arr2[i] * arr2[i]
// 	}
// 	a := math.Sqrt(float64(total_a))
// 	b := math.Sqrt(float64(total_b))
// 	return float64(total) / a / b
// }

func GenerateColorHistogramhHash(img image.Image) []float32 {
	var totol_arr [4096]float32
	for i := 0; i < 4096; i++ {
		totol_arr[i] = 0
	}
	// fmt.Println(img)
	dx := img.Bounds().Dx()
	dy := img.Bounds().Dy()
	for i := 0; i < dx; i++ {
		for j := 0; j < dy; j++ {
			r, g, b, _ := img.At(i, j).RGBA()
			// fmt.Println(r, g, b)
			index := divide_color_into_bins(r>>8) + divide_color_into_bins(g>>8)*16 + divide_color_into_bins(b>>8)*256
			totol_arr[index]++
		}
	}
	return totol_arr[:]
}

func divide_color_into_bins(value uint32) uint32 {
	return value / 16
}

// func Similar(img1, img2 image.Image) float64 {
// 	hash1 := Generate_color_histogram_hash(img1)
// 	hash2 := Generate_color_histogram_hash(img2)
// 	return Cosine_similarity(hash1, hash2)
// }

func ImgToIconHash(img image.Image) []float32 {
	icon := images4.Icon(img)
	// fmt.Println(len(icon.Pixels))
	var totol_arr [363]float32
	for key, _ := range icon.Pixels {
		totol_arr[key] = float32(icon.Pixels[key])
	}
	return totol_arr[:]
}
