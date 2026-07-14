package redismetric

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/gocql/gocql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"gonum.org/v1/gonum/mat"
)

type MetricData struct {
	NODE_ID     string  `json:"node_id"`
	CPUUsageMax float64 `json:"cpu_usage_Max"`
	CPUUsageMin float64 `json:"cpu_usage_Min"`
	CPUUsageAVG float64 `json:"cpu_usage_AVG"`

	MemoryUsageMax float64 `json:"memory_usage_max"`
	MemoryUsageMin float64 `json:"memory_usage_min"`
	MemoryUsageAVG float64 `json:"memory_usage_AVG"`

	TailLatencyMax float64 `json:"tail_latency_max"`
	TailLatencyMin float64 `json:"tail_latency_min"`
	TailLatencyAVG float64 `json:"tail_latency_avg"`

	DiskFreeMax float64 `json:"disk_free_Max"`
	DiskFreeMin float64 `json:"disk_free_Min"`
	DiskFreeAVG float64 `json:"disk_free_AVG"`

	SwapMemory_max float64 `json:"swap_memory_max"`
	SwapMemory_min float64 `json:"swap_memory_min"`
	SwapMemory_avg float64 `json:"swap_memory_avg"`

	DiskIOCounterBusyTimeMax float64 `json:"disk_io_counter_busy_time_max"`
	DiskIOCounterBusyTimeMin float64 `json:"disk_io_counter_busy_time_min"`
	DiskIOCounterBusyTimeAVG float64 `json:"disk_io_counter_busy_time_avg"`

	NetIOCounterDropinMax float64 `json:"net_io_counter_dropin_max"`
	NetIOCounterDropinMin float64 `json:"net_io_counter_dropin_min"`
	NetIOCounterDropinAVG float64 `json:"net_io_counter_dropin_avg"`

	NetIOCounterDropoutMax float64 `json:"net_io_counter_dropout_max"`
	NetIOCounterDropoutMin float64 `json:"net_io_counter_dropout_min"`
	NetIOCounterDropoutAVG float64 `json:"net_io_counter_dropout_avg"`
}

type Metrics []MetricData

var data_Metrics Metrics

type Object struct {
	MetricDatas Metrics
}

const Total_Node int = 3
const Total_criteria = 8

var rdb *redis.Client
var db *sql.DB
var GobalMin_CPUUsage float64
var GobalMin_MemoryUsage float64
var GobalMin_TailLatency float64
var GobalMax_DiskFree float64
var GobalMin_SwapMemory float64
var GobalMin_DiskIOCounterBusyTime float64
var GobalMin_NetIOCounterDropin float64
var GobalMin_NetIOCounterDropout float64

var Aw_CPUUsage []float64
var Ab_CPUUsage []float64
var Aw_MemoryUsage []float64
var Ab_MemoryUsage []float64
var Aw_TailLatency []float64
var Ab_TailLatency []float64
var Aw_DiskFree []float64
var Ab_DiskFree []float64
var Aw_SwapMemory []float64
var Ab_SwapMemory []float64
var Aw_DiskIOCounterBusyTime []float64
var Ab_DiskIOCounterBusyTime []float64
var Aw_NetIOCounterDropin []float64
var Ab_NetIOCounterDropin []float64
var Aw_NetIOCounterDropout []float64
var Ab_NetIOCounterDropout []float64

var Weight_CPUUsage float64
var Weight_MemoryUsage float64
var Weight_TailLatency float64
var Weight_DiskFree float64
var Weight_SwapMemory float64
var Weight_DiskIOCounterBusyTime float64
var Weight_NetIOCounterDropin float64
var Weight_NetIOCounterDropout float64

type Object_Metrix struct {
	name_node string
	score     float64
}
type WeightCriteria struct {
	name_criteria string
	score         float64
}

var result_object = make([]Object_Metrix, Total_Node)

const epsilon = 0.001

func norm_matrix(data Metrics) Metrics {
	// FIX #1: khởi tạo các điểm neo bằng giá trị thực của node đầu tiên.
	// Trước đây các biến này khởi tạo mặc định = 0, và điều kiện so sánh
	// "< GobalMin" hay "> GobalMax" với dữ liệu dương không bao giờ đúng,
	// khiến các điểm neo không phản ánh min/max thật của dữ liệu.
	GobalMin_CPUUsage = data[0].CPUUsageMin
	GobalMin_MemoryUsage = data[0].MemoryUsageMin
	GobalMin_TailLatency = data[0].TailLatencyMin
	GobalMax_DiskFree = data[0].DiskFreeMax
	GobalMin_SwapMemory = data[0].SwapMemory_min
	GobalMin_DiskIOCounterBusyTime = data[0].DiskIOCounterBusyTimeMin
	GobalMin_NetIOCounterDropin = data[0].NetIOCounterDropinMin
	GobalMin_NetIOCounterDropout = data[0].NetIOCounterDropoutMin

	for i := 0; i < Total_Node; i++ {
		for j := 0; j < Total_criteria; j++ {
			switch j {
			case 0:

				if data[i].CPUUsageMin < GobalMin_CPUUsage {
					GobalMin_CPUUsage = data[i].CPUUsageMin
				}
			case 1:

				if data[i].MemoryUsageMin < GobalMin_MemoryUsage {
					GobalMin_MemoryUsage = data[i].MemoryUsageMin
				}
			case 2:

				if data[i].TailLatencyMin < GobalMin_TailLatency {
					GobalMin_TailLatency = data[i].TailLatencyMin
				}
			case 3:
				if data[i].DiskFreeMax > GobalMax_DiskFree {
					GobalMax_DiskFree = data[i].DiskFreeMax
				}

			case 4:

				if data[i].SwapMemory_min < GobalMin_SwapMemory {
					GobalMin_SwapMemory = data[i].SwapMemory_min
				}
			case 5:

				if data[i].DiskIOCounterBusyTimeMin < GobalMin_DiskIOCounterBusyTime {
					GobalMin_DiskIOCounterBusyTime = data[i].DiskIOCounterBusyTimeMin
				}
			case 6:

				if data[i].NetIOCounterDropinMin < GobalMin_NetIOCounterDropin {
					GobalMin_NetIOCounterDropin = data[i].NetIOCounterDropinMin
				}
			case 7:

				if data[i].NetIOCounterDropoutMin < GobalMin_NetIOCounterDropout {
					GobalMin_NetIOCounterDropout = data[i].NetIOCounterDropoutMin
				}
			}

		}
	}
	if GobalMin_CPUUsage == 0 {
		GobalMin_CPUUsage = 0.01
	}

	if GobalMax_DiskFree == 0 {
		GobalMax_DiskFree = 0.01
	}
	if GobalMin_DiskIOCounterBusyTime == 0 {
		GobalMin_DiskIOCounterBusyTime = 0.01
	}
	if GobalMin_MemoryUsage == 0 {
		GobalMin_MemoryUsage = 0.01
	}
	if GobalMin_NetIOCounterDropin == 0 {
		GobalMin_NetIOCounterDropin = 0.01
	}
	if GobalMin_NetIOCounterDropout == 0 {
		GobalMin_NetIOCounterDropout = 0.01
	}
	// FIX #2 (phần 1): thêm fallback còn thiếu cho 2 biến này để tránh chia cho 0
	if GobalMin_TailLatency == 0 {
		GobalMin_TailLatency = 0.01
	}
	if GobalMin_SwapMemory == 0 {
		GobalMin_SwapMemory = 0.01
	}

	// FIX #3: xoá vòng lặp lồng dùng trùng tên biến "i" (trước đây khiến toàn bộ
	// khối chuẩn hoá bên dưới bị lặp lại 3 lần thay vì 1 lần cho mỗi phần tử).
	for i := 0; i < Total_Node; i++ {
		// CPU Usage (càng nhỏ càng tốt -> normalize về [0,1] với 1 là tốt nhất)
		if data[i].CPUUsageAVG > 0 {
			data[i].CPUUsageAVG = GobalMin_CPUUsage / data[i].CPUUsageAVG
			if data[i].CPUUsageAVG == 0 {
				data[i].CPUUsageAVG = epsilon
			}
		}

		if data[i].CPUUsageMax > 0 {
			data[i].CPUUsageMax = GobalMin_CPUUsage / data[i].CPUUsageMax
			if data[i].CPUUsageMax == 0 {
				data[i].CPUUsageMax = epsilon
			}
		}

		if data[i].CPUUsageMin > 0 {
			data[i].CPUUsageMin = GobalMin_CPUUsage / data[i].CPUUsageMin
			if data[i].CPUUsageMin == 0 {
				data[i].CPUUsageMin = epsilon
			}
		}

		// Disk Free (càng lớn càng tốt -> chia cho max)
		if GobalMax_DiskFree > 0 {
			data[i].DiskFreeAVG = data[i].DiskFreeAVG / GobalMax_DiskFree
			data[i].DiskFreeMax = data[i].DiskFreeMax / GobalMax_DiskFree
			data[i].DiskFreeMin = data[i].DiskFreeMin / GobalMax_DiskFree

			// Đảm bảo không âm
			if data[i].DiskFreeAVG < 0 {
				data[i].DiskFreeAVG = epsilon
			}
			if data[i].DiskFreeMax < 0 {
				data[i].DiskFreeMax = epsilon
			}
			if data[i].DiskFreeMin < 0 {
				data[i].DiskFreeMin = epsilon
			}
		}

		// Disk IO Busy Time (càng nhỏ càng tốt)
		if data[i].DiskIOCounterBusyTimeAVG > 0 {
			data[i].DiskIOCounterBusyTimeAVG = GobalMin_DiskIOCounterBusyTime / data[i].DiskIOCounterBusyTimeAVG
			if data[i].DiskIOCounterBusyTimeAVG == 0 {
				data[i].DiskIOCounterBusyTimeAVG = epsilon
			}
		}

		if data[i].DiskIOCounterBusyTimeMax > 0 {
			data[i].DiskIOCounterBusyTimeMax = GobalMin_DiskIOCounterBusyTime / data[i].DiskIOCounterBusyTimeMax
			if data[i].DiskIOCounterBusyTimeMax == 0 {
				data[i].DiskIOCounterBusyTimeMax = epsilon
			}
		}

		if data[i].DiskIOCounterBusyTimeMin > 0 {
			data[i].DiskIOCounterBusyTimeMin = GobalMin_DiskIOCounterBusyTime / data[i].DiskIOCounterBusyTimeMin
			if data[i].DiskIOCounterBusyTimeMin == 0 {
				data[i].DiskIOCounterBusyTimeMin = epsilon
			}
		}

		// Memory Usage (càng nhỏ càng tốt)
		if data[i].MemoryUsageAVG > 0 {
			data[i].MemoryUsageAVG = GobalMin_MemoryUsage / data[i].MemoryUsageAVG
			if data[i].MemoryUsageAVG == 0 {
				data[i].MemoryUsageAVG = epsilon
			}
		}

		if data[i].MemoryUsageMax > 0 {
			data[i].MemoryUsageMax = GobalMin_MemoryUsage / data[i].MemoryUsageMax
			if data[i].MemoryUsageMax == 0 {
				data[i].MemoryUsageMax = epsilon
			}
		}

		if data[i].MemoryUsageMin > 0 {
			data[i].MemoryUsageMin = GobalMin_MemoryUsage / data[i].MemoryUsageMin
			if data[i].MemoryUsageMin == 0 {
				data[i].MemoryUsageMin = epsilon
			}
		}

		// Net IO Dropin (càng nhỏ càng tốt)
		if data[i].NetIOCounterDropinAVG > 0 {
			data[i].NetIOCounterDropinAVG = GobalMin_NetIOCounterDropin / data[i].NetIOCounterDropinAVG
			if data[i].NetIOCounterDropinAVG == 0 {
				data[i].NetIOCounterDropinAVG = epsilon
			}
		}

		if data[i].NetIOCounterDropinMax > 0 {
			data[i].NetIOCounterDropinMax = GobalMin_NetIOCounterDropin / data[i].NetIOCounterDropinMax
			if data[i].NetIOCounterDropinMax == 0 {
				data[i].NetIOCounterDropinMax = epsilon
			}
		}

		if data[i].NetIOCounterDropinMin > 0 {
			data[i].NetIOCounterDropinMin = GobalMin_NetIOCounterDropin / data[i].NetIOCounterDropinMin
			if data[i].NetIOCounterDropinMin == 0 {
				data[i].NetIOCounterDropinMin = epsilon
			}
		}

		// Net IO Dropout (càng nhỏ càng tốt)
		if data[i].NetIOCounterDropoutAVG > 0 {
			data[i].NetIOCounterDropoutAVG = GobalMin_NetIOCounterDropout / data[i].NetIOCounterDropoutAVG
			if data[i].NetIOCounterDropoutAVG == 0 {
				data[i].NetIOCounterDropoutAVG = epsilon
			}
		}

		if data[i].NetIOCounterDropoutMax > 0 {
			data[i].NetIOCounterDropoutMax = GobalMin_NetIOCounterDropout / data[i].NetIOCounterDropoutMax
			if data[i].NetIOCounterDropoutMax == 0 {
				data[i].NetIOCounterDropoutMax = epsilon
			}
		}

		if data[i].NetIOCounterDropoutMin > 0 {
			data[i].NetIOCounterDropoutMin = GobalMin_NetIOCounterDropout / data[i].NetIOCounterDropoutMin
			if data[i].NetIOCounterDropoutMin == 0 {
				data[i].NetIOCounterDropoutMin = epsilon
			}
		}

		// FIX #2 (phần 2): trước đây TailLatency hoàn toàn không được chuẩn hoá,
		// giữ nguyên thang đo gốc (chục/trăm) trong khi các tiêu chí khác đã co
		// về khoảng 0-1, khiến TailLatency áp đảo khoảng cách sau này.
		// Tail Latency (càng nhỏ càng tốt)
		if data[i].TailLatencyAVG > 0 {
			data[i].TailLatencyAVG = GobalMin_TailLatency / data[i].TailLatencyAVG
			if data[i].TailLatencyAVG == 0 {
				data[i].TailLatencyAVG = epsilon
			}
		}
		if data[i].TailLatencyMax > 0 {
			data[i].TailLatencyMax = GobalMin_TailLatency / data[i].TailLatencyMax
			if data[i].TailLatencyMax == 0 {
				data[i].TailLatencyMax = epsilon
			}
		}
		if data[i].TailLatencyMin > 0 {
			data[i].TailLatencyMin = GobalMin_TailLatency / data[i].TailLatencyMin
			if data[i].TailLatencyMin == 0 {
				data[i].TailLatencyMin = epsilon
			}
		}

		// FIX #2 (phần 2): tương tự, SwapMemory trước đây cũng bị bỏ sót.
		// Swap Memory (càng nhỏ càng tốt)
		if data[i].SwapMemory_avg > 0 {
			data[i].SwapMemory_avg = GobalMin_SwapMemory / data[i].SwapMemory_avg
			if data[i].SwapMemory_avg == 0 {
				data[i].SwapMemory_avg = epsilon
			}
		}
		if data[i].SwapMemory_max > 0 {
			data[i].SwapMemory_max = GobalMin_SwapMemory / data[i].SwapMemory_max
			if data[i].SwapMemory_max == 0 {
				data[i].SwapMemory_max = epsilon
			}
		}
		if data[i].SwapMemory_min > 0 {
			data[i].SwapMemory_min = GobalMin_SwapMemory / data[i].SwapMemory_min
			if data[i].SwapMemory_min == 0 {
				data[i].SwapMemory_min = epsilon
			}
		}
	}
	return data
}

func triple(a, b, c float64) []float64 { return []float64{a, b, c} }

// FIX #6/#7: Vì norm_matrix đã tự đảo chiều cost -> benefit (min/x cho cost,
// x/max cho benefit), MỌI t_ij sau chuẩn hoá đều đã cùng chiều "càng lớn càng tốt".
// Do đó A_w (worst) phải luôn lấy giá trị NHỎ NHẤT và A_b (best) phải luôn lấy
// giá trị LỚN NHẤT theo từng thành phần (Max/AVG/Min), áp dụng THỐNG NHẤT cho
// cả 8 tiêu chí — không còn phân biệt J+/J- (benefit/cost) ở bước này nữa.
// Bản cũ áp dụng công thức J+/J- gốc (vốn dành cho chuẩn hoá KHÔNG đảo chiều)
// lên dữ liệu ĐÃ đảo chiều, khiến Aw/Ab bị gán ngược nhãn cho 7/8 tiêu chí
// (chỉ DiskFree, tiêu chí benefit duy nhất không bị đảo, là đúng một cách tình cờ).
func worst_best_alternative(norm_data Metrics) {
	minMax := func(get func(MetricData) (float64, float64, float64)) (worst, best []float64) {
		maxV, avgV, minV := get(norm_data[0])
		wMax, wAvg, wMin := maxV, avgV, minV
		bMax, bAvg, bMin := maxV, avgV, minV
		for _, d := range norm_data {
			maxV, avgV, minV = get(d)
			if maxV < wMax {
				wMax = maxV
			}
			if avgV < wAvg {
				wAvg = avgV
			}
			if minV < wMin {
				wMin = minV
			}
			if maxV > bMax {
				bMax = maxV
			}
			if avgV > bAvg {
				bAvg = avgV
			}
			if minV > bMin {
				bMin = minV
			}
		}
		return triple(wMax, wAvg, wMin), triple(bMax, bAvg, bMin)
	}

	Aw_CPUUsage, Ab_CPUUsage = minMax(func(d MetricData) (float64, float64, float64) {
		return d.CPUUsageMax, d.CPUUsageAVG, d.CPUUsageMin
	})
	Aw_MemoryUsage, Ab_MemoryUsage = minMax(func(d MetricData) (float64, float64, float64) {
		return d.MemoryUsageMax, d.MemoryUsageAVG, d.MemoryUsageMin
	})
	Aw_TailLatency, Ab_TailLatency = minMax(func(d MetricData) (float64, float64, float64) {
		return d.TailLatencyMax, d.TailLatencyAVG, d.TailLatencyMin
	})
	Aw_DiskFree, Ab_DiskFree = minMax(func(d MetricData) (float64, float64, float64) {
		return d.DiskFreeMax, d.DiskFreeAVG, d.DiskFreeMin
	})
	Aw_SwapMemory, Ab_SwapMemory = minMax(func(d MetricData) (float64, float64, float64) {
		return d.SwapMemory_max, d.SwapMemory_avg, d.SwapMemory_min
	})
	Aw_DiskIOCounterBusyTime, Ab_DiskIOCounterBusyTime = minMax(func(d MetricData) (float64, float64, float64) {
		return d.DiskIOCounterBusyTimeMax, d.DiskIOCounterBusyTimeAVG, d.DiskIOCounterBusyTimeMin
	})
	Aw_NetIOCounterDropin, Ab_NetIOCounterDropin = minMax(func(d MetricData) (float64, float64, float64) {
		return d.NetIOCounterDropinMax, d.NetIOCounterDropinAVG, d.NetIOCounterDropinMin
	})
	Aw_NetIOCounterDropout, Ab_NetIOCounterDropout = minMax(func(d MetricData) (float64, float64, float64) {
		return d.NetIOCounterDropoutMax, d.NetIOCounterDropoutAVG, d.NetIOCounterDropoutMin
	})
}

func metricAvgByCriteria(criteriaIdx int, sample MetricData) float64 {
	switch criteriaIdx {
	case 0:
		return sample.CPUUsageAVG
	case 1:
		return sample.MemoryUsageAVG
	case 2:
		return sample.TailLatencyAVG
	case 3:
		return sample.DiskFreeAVG
	case 4:
		return sample.SwapMemory_avg
	case 5:
		return sample.DiskIOCounterBusyTimeAVG
	case 6:
		return sample.NetIOCounterDropinAVG
	case 7:
		return sample.NetIOCounterDropoutAVG
	default:
		return 0
	}
}

// Sigma_Yij returns sum_i(y_ij) for one criterion j across all samples i.
func Sigma_Yij(criteriaIdx int, data Metrics) float64 {
	var sum float64
	for i := 0; i < len(data); i++ {
		sum += metricAvgByCriteria(criteriaIdx, data[i])
	}
	return sum
}

// Sigma_Pij_ln_Pij returns sum_i(p_ij * ln(p_ij)) for one criterion j.
func Sigma_Pij_ln_Pij(criteriaIdx int, data Metrics) float64 {
	var sum float64
	sigmaYj := Sigma_Yij(criteriaIdx, data)
	if sigmaYj <= 0 {
		return 0
	}

	for i := 0; i < len(data); i++ {
		pij := metricAvgByCriteria(criteriaIdx, data[i]) / sigmaYj
		if pij > 0 {
			sum += pij * math.Log(pij)
		}
	}
	return sum
}

// Ej computes entropy of criterion j: e_j = -(1/ln(m)) * sum_i(p_ij * ln(p_ij)).
func Ej(criteriaIdx int, data Metrics) float64 {
	m := len(data)
	if m <= 1 {
		return 0
	}

	k := 1 / math.Log(float64(m))
	return -k * Sigma_Pij_ln_Pij(criteriaIdx, data)
}

// Entropy_w computes entropy weights x_j = (1-e_j)/sum_j(1-e_j).
func Entropy_w(data Metrics) []float64 {
	weights := make([]float64, Total_criteria)
	entropyValues := make([]float64, Total_criteria)

	var denom float64
	for j := 0; j < Total_criteria; j++ {
		e := Ej(j, data)
		entropyValues[j] = e
		denom += 1 - e
	}

	if denom == 0 {
		uniform := 1 / float64(Total_criteria)
		for j := 0; j < Total_criteria; j++ {
			weights[j] = uniform
		}
	} else {
		for j := 0; j < Total_criteria; j++ {
			weights[j] = (1 - entropyValues[j]) / denom
		}
	}

	Weight_CPUUsage = weights[0]
	Weight_MemoryUsage = weights[1]
	Weight_TailLatency = weights[2]
	Weight_DiskFree = weights[3]
	Weight_SwapMemory = weights[4]
	Weight_DiskIOCounterBusyTime = weights[5]
	Weight_NetIOCounterDropin = weights[6]
	Weight_NetIOCounterDropout = weights[7]

	return weights
}

func normlised_decision_matrix(Weight []float64, Data Metrics) Metrics {
	for i := 0; i < Total_Node; i++ {
		Data[i].CPUUsageMax = Data[i].CPUUsageMax * Weight[0]
		Data[i].CPUUsageAVG = Data[i].CPUUsageAVG * Weight[0]
		Data[i].CPUUsageMin = Data[i].CPUUsageMin * Weight[0]

		Data[i].MemoryUsageMax = Data[i].MemoryUsageMax * Weight[1]
		Data[i].MemoryUsageAVG = Data[i].MemoryUsageAVG * Weight[1]
		Data[i].MemoryUsageMin = Data[i].MemoryUsageMin * Weight[1]

		Data[i].TailLatencyMax = Data[i].TailLatencyMax * Weight[2]
		Data[i].TailLatencyAVG = Data[i].TailLatencyAVG * Weight[2]
		Data[i].TailLatencyMin = Data[i].TailLatencyMin * Weight[2]

		Data[i].DiskFreeMax = Data[i].DiskFreeMax * Weight[3]
		Data[i].DiskFreeAVG = Data[i].DiskFreeAVG * Weight[3]
		Data[i].DiskFreeMin = Data[i].DiskFreeMin * Weight[3]

		Data[i].SwapMemory_max = Data[i].SwapMemory_max * Weight[4]
		Data[i].SwapMemory_avg = Data[i].SwapMemory_avg * Weight[4]
		Data[i].SwapMemory_min = Data[i].SwapMemory_min * Weight[4]

		Data[i].DiskIOCounterBusyTimeMax = Data[i].DiskIOCounterBusyTimeMax * Weight[5]
		Data[i].DiskIOCounterBusyTimeAVG = Data[i].DiskIOCounterBusyTimeAVG * Weight[5]
		Data[i].DiskIOCounterBusyTimeMin = Data[i].DiskIOCounterBusyTimeMin * Weight[5]

		Data[i].NetIOCounterDropinMax = Data[i].NetIOCounterDropinMax * Weight[6]
		Data[i].NetIOCounterDropinAVG = Data[i].NetIOCounterDropinAVG * Weight[6]
		Data[i].NetIOCounterDropinMin = Data[i].NetIOCounterDropinMin * Weight[6]

		Data[i].NetIOCounterDropoutMax = Data[i].NetIOCounterDropoutMax * Weight[7]
		Data[i].NetIOCounterDropoutAVG = Data[i].NetIOCounterDropoutAVG * Weight[7]
		Data[i].NetIOCounterDropoutMin = Data[i].NetIOCounterDropoutMin * Weight[7]

	}
	return Data
}
func Diw_Sigma_Tij_Twj(i int, cpuUsage_matrix *mat.Dense, memoryUsage_matrix *mat.Dense, tailLatency_matrix *mat.Dense, diskFree_matrix *mat.Dense, swapMemory_matrix *mat.Dense, diskIOBusyTime_matrix *mat.Dense, netIODropin_matrix *mat.Dense, netIODropout_matrix *mat.Dense) float64 {
	var result float64 = 0
	for j := 0; j < Total_criteria; j++ {
		if j == 0 {
			var diff mat.Dense
			diff.Sub(cpuUsage_matrix, mat.NewDense(1, 3, Aw_CPUUsage))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 1 {
			var diff mat.Dense
			diff.Sub(memoryUsage_matrix, mat.NewDense(1, 3, Aw_MemoryUsage))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 2 {
			var diff mat.Dense
			diff.Sub(tailLatency_matrix, mat.NewDense(1, 3, Aw_TailLatency))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 3 {
			var diff mat.Dense
			diff.Sub(diskFree_matrix, mat.NewDense(1, 3, Aw_DiskFree))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 4 {
			var diff mat.Dense
			diff.Sub(swapMemory_matrix, mat.NewDense(1, 3, Aw_SwapMemory))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 5 {
			var diff mat.Dense
			diff.Sub(diskIOBusyTime_matrix, mat.NewDense(1, 3, Aw_DiskIOCounterBusyTime))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 6 {
			var diff mat.Dense
			diff.Sub(netIODropin_matrix, mat.NewDense(1, 3, Aw_NetIOCounterDropin))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 7 {
			var diff mat.Dense
			diff.Sub(netIODropout_matrix, mat.NewDense(1, 3, Aw_NetIOCounterDropout))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		}
	}
	return result
}

func Dib_Sigma_Tij_Tbj(i int, cpuUsage_matrix *mat.Dense, memoryUsage_matrix *mat.Dense, tailLatency_matrix *mat.Dense, diskFree_matrix *mat.Dense, swapMemory_matrix *mat.Dense, diskIOBusyTime_matrix *mat.Dense, netIODropin_matrix *mat.Dense, netIODropout_matrix *mat.Dense) float64 {
	var result float64 = 0
	for j := 0; j < Total_criteria; j++ {
		if j == 0 {
			var diff mat.Dense
			diff.Sub(cpuUsage_matrix, mat.NewDense(1, 3, Ab_CPUUsage))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 1 {
			var diff mat.Dense
			diff.Sub(memoryUsage_matrix, mat.NewDense(1, 3, Ab_MemoryUsage))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 2 {
			var diff mat.Dense
			diff.Sub(tailLatency_matrix, mat.NewDense(1, 3, Ab_TailLatency))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 3 {
			var diff mat.Dense
			diff.Sub(diskFree_matrix, mat.NewDense(1, 3, Ab_DiskFree))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 4 {
			var diff mat.Dense
			diff.Sub(swapMemory_matrix, mat.NewDense(1, 3, Ab_SwapMemory))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 5 {
			var diff mat.Dense
			diff.Sub(diskIOBusyTime_matrix, mat.NewDense(1, 3, Ab_DiskIOCounterBusyTime))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 6 {
			var diff mat.Dense
			diff.Sub(netIODropin_matrix, mat.NewDense(1, 3, Ab_NetIOCounterDropin))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		} else if j == 7 {
			var diff mat.Dense
			diff.Sub(netIODropout_matrix, mat.NewDense(1, 3, Ab_NetIOCounterDropout))
			var c mat.Dense
			diffT := diff.T()
			c.Mul(&diff, diffT)
			result += c.At(0, 0)
		}
	}
	return result
}
func Score(data Metrics) int {
	var Score = make([]float64, Total_Node)
	for i := 0; i < Total_Node; i++ {
		var cpuUsage = make([]float64, 3)
		cpuUsage[0] = data[i].CPUUsageMax
		cpuUsage[1] = data[i].CPUUsageAVG
		cpuUsage[2] = data[i].CPUUsageMin

		var memoryUsage = make([]float64, 3)
		memoryUsage[0] = data[i].MemoryUsageMax
		memoryUsage[1] = data[i].MemoryUsageAVG
		memoryUsage[2] = data[i].MemoryUsageMin

		var tailLatency = make([]float64, 3)
		tailLatency[0] = data[i].TailLatencyMax
		tailLatency[1] = data[i].TailLatencyAVG
		tailLatency[2] = data[i].TailLatencyMin

		var diskFree = make([]float64, 3)
		diskFree[0] = data[i].DiskFreeMax
		diskFree[1] = data[i].DiskFreeAVG
		diskFree[2] = data[i].DiskFreeMin

		var swapMemory = make([]float64, 3)
		swapMemory[0] = data[i].SwapMemory_max
		swapMemory[1] = data[i].SwapMemory_avg
		swapMemory[2] = data[i].SwapMemory_min

		var diskIOBusyTime = make([]float64, 3)
		diskIOBusyTime[0] = data[i].DiskIOCounterBusyTimeMax
		diskIOBusyTime[1] = data[i].DiskIOCounterBusyTimeAVG
		diskIOBusyTime[2] = data[i].DiskIOCounterBusyTimeMin

		var netIODropin = make([]float64, 3)
		netIODropin[0] = data[i].NetIOCounterDropinMax
		netIODropin[1] = data[i].NetIOCounterDropinAVG
		netIODropin[2] = data[i].NetIOCounterDropinMin

		var netIODropout = make([]float64, 3)
		netIODropout[0] = data[i].NetIOCounterDropoutMax
		netIODropout[1] = data[i].NetIOCounterDropoutAVG
		netIODropout[2] = data[i].NetIOCounterDropoutMin

		cpuUsage_matrix := mat.NewDense(1, 3, cpuUsage)
		memoryUsage_matrix := mat.NewDense(1, 3, memoryUsage)
		tailLatency_matrix := mat.NewDense(1, 3, tailLatency)
		diskFree_matrix := mat.NewDense(1, 3, diskFree)
		swapMemory_matrix := mat.NewDense(1, 3, swapMemory)
		diskIOBusyTime_matrix := mat.NewDense(1, 3, diskIOBusyTime)
		netIODropin_matrix := mat.NewDense(1, 3, netIODropin)
		netIODropout_matrix := mat.NewDense(1, 3, netIODropout)

		var Diw = math.Sqrt(Diw_Sigma_Tij_Twj(i, cpuUsage_matrix, memoryUsage_matrix, tailLatency_matrix, diskFree_matrix, swapMemory_matrix, diskIOBusyTime_matrix, netIODropin_matrix, netIODropout_matrix))
		var Dib = math.Sqrt(Dib_Sigma_Tij_Tbj(i, cpuUsage_matrix, memoryUsage_matrix, tailLatency_matrix, diskFree_matrix, swapMemory_matrix, diskIOBusyTime_matrix, netIODropin_matrix, netIODropout_matrix))
		var Siw = Diw / (Diw + Dib)
		// fmt.Print("\n Diw: %s \n", Diw)
		// fmt.Print("\n Dib: %s \n", Dib)
		// fmt.Print("\n Siw: %s \n", Siw)
		// fmt.Print("\n ------------------------------------- \n")
		Score[i] = Siw

	}
	fmt.Print(Score)
	var max_score float64
	var index int = 0
	for i, score := range Score {
		if score > max_score {
			max_score = score
			index = i
		}
	}

	return index

}

func CalculateTopNodeFromScylla(ctx context.Context, session *gocql.Session) (string, error) {
	fmt.Print("\nhello\n")

	// Đã thay thế SELECT * bằng các cột cụ thể, theo ĐÚNG thứ tự của hàm Scan bên dưới
	query := `
    SELECT node_id, 
           "CPUUsageMax", "CPUUsageMin", "CPUUsageAVG",
           "MemoryUsageMax", "MemoryUsageMin", "MemoryUsageAVG",
           "TailLatencyMax", "TailLatencyMin", "TailLatencyAVG",
           "DiskFreeMax", "DiskFreeMin", "DiskFreeAVG",
           "SwapMemory_max", "SwapMemory_min", "SwapMemory_avg",
           "DiskIOCounterBusyTimeMax", "DiskIOCounterBusyTimeMin", "DiskIOCounterBusyTimeAVG",
           "NetIOCounterDropinMax", "NetIOCounterDropinMin", "NetIOCounterDropinAVG",
           "NetIOCounterDropoutMax", "NetIOCounterDropoutMin", "NetIOCounterDropoutAVG"
    FROM metricdata;
    `

	iter := session.Query(query).WithContext(ctx).Iter()
	defer iter.Close()

	var dataMetrics []MetricData

	scanner := iter.Scanner()
	for scanner.Next() {
		var data MetricData

		// Thứ tự các biến ở đây phải khớp 100% với thứ tự cột trong câu SELECT ở trên
		err := scanner.Scan(
			&data.NODE_ID,
			&data.CPUUsageMax, &data.CPUUsageMin, &data.CPUUsageAVG,
			&data.MemoryUsageMax, &data.MemoryUsageMin, &data.MemoryUsageAVG,
			&data.TailLatencyMax, &data.TailLatencyMin, &data.TailLatencyAVG,
			&data.DiskFreeMax, &data.DiskFreeMin, &data.DiskFreeAVG,
			&data.SwapMemory_max, &data.SwapMemory_min, &data.SwapMemory_avg,
			&data.DiskIOCounterBusyTimeMax, &data.DiskIOCounterBusyTimeMin, &data.DiskIOCounterBusyTimeAVG,
			&data.NetIOCounterDropinMax, &data.NetIOCounterDropinMin, &data.NetIOCounterDropinAVG,
			&data.NetIOCounterDropoutMax, &data.NetIOCounterDropoutMin, &data.NetIOCounterDropoutAVG,
		)
		if err != nil {
			fmt.Printf("❌ Lỗi Scan dòng dữ liệu: %v\n", err)
			continue // Nếu lỗi sẽ in ra log và tiếp tục vòng lặp
		}
		dataMetrics = append(dataMetrics, data)
	}

	if err := iter.Close(); err != nil {
		return "", fmt.Errorf("query error: %w", err)
	}

	if len(dataMetrics) == 0 {
		return "", fmt.Errorf("no metric data found")
	}

	// Dùng cùng logic tính toán từ code gốc
	normalizedMetrics := norm_matrix(dataMetrics)
	entropyWeights := Entropy_w(normalizedMetrics)
	decisionMatrix := normlised_decision_matrix(entropyWeights, normalizedMetrics)
	worst_best_alternative(decisionMatrix)

	// Tìm node tốt nhất
	bestIndex := Score(decisionMatrix)
	bestNodeName := fmt.Sprintf("node%d", bestIndex)

	return bestNodeName, nil
}
