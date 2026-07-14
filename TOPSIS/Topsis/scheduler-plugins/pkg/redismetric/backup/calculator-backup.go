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

	for i := 0; i < Total_Node; i++ {
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
		}
	}
	return data
}

func worst_best_alternative(norm_data Metrics) {
	var Aw_CPUUsage_num float64 = norm_data[0].CPUUsageMax
	var Ab_CPUUsage_num float64 = norm_data[0].CPUUsageMin
	var Aw_MemoryUsage_num float64 = norm_data[0].MemoryUsageMax
	var Ab_MemoryUsage_num float64 = norm_data[0].MemoryUsageMin
	var Aw_TailLatency_num float64 = norm_data[0].TailLatencyMax
	var Ab_TailLatency_num float64 = norm_data[0].TailLatencyMin
	var Aw_DiskFree_num float64 = norm_data[0].DiskFreeMin
	var Ab_DiskFree_num float64 = norm_data[0].DiskFreeMax
	var Aw_SwapMemory_num float64 = norm_data[0].DiskFreeMax
	var Ab_SwapMemory_num float64 = norm_data[0].DiskFreeMin
	var Aw_DiskIOCounterBusyTime_num float64 = norm_data[0].DiskIOCounterBusyTimeMax
	var Ab_DiskIOCounterBusyTime_num float64 = norm_data[0].DiskIOCounterBusyTimeMin
	var Aw_NetIOCounterDropin_num float64 = norm_data[0].NetIOCounterDropinMax
	var Ab_NetIOCounterDropin_num float64 = norm_data[0].NetIOCounterDropinMin
	var Aw_NetIOCounterDropout_num float64 = norm_data[0].NetIOCounterDropoutMax
	var Ab_NetIOCounterDropout_num float64 = norm_data[0].NetIOCounterDropoutMin
	Aw_CPUUsage = []float64{norm_data[0].CPUUsageMax, norm_data[0].CPUUsageAVG, norm_data[0].CPUUsageMin}
	Ab_CPUUsage = []float64{norm_data[0].CPUUsageMax, norm_data[0].CPUUsageAVG, norm_data[0].CPUUsageMin}
	Aw_MemoryUsage = []float64{norm_data[0].MemoryUsageMax, norm_data[0].MemoryUsageAVG, norm_data[0].MemoryUsageMin}
	Ab_MemoryUsage = []float64{norm_data[0].MemoryUsageMax, norm_data[0].MemoryUsageAVG, norm_data[0].MemoryUsageMin}
	Aw_TailLatency = []float64{norm_data[0].TailLatencyMax, norm_data[0].TailLatencyAVG, norm_data[0].TailLatencyMin}
	Ab_TailLatency = []float64{norm_data[0].TailLatencyMax, norm_data[0].TailLatencyAVG, norm_data[0].TailLatencyMin}
	Aw_DiskFree = []float64{norm_data[0].DiskFreeMax, norm_data[0].DiskFreeAVG, norm_data[0].DiskFreeMin}
	Ab_DiskFree = []float64{norm_data[0].DiskFreeMax, norm_data[0].DiskFreeAVG, norm_data[0].DiskFreeMin}
	Aw_SwapMemory = []float64{norm_data[0].SwapMemory_max, norm_data[0].SwapMemory_avg, norm_data[0].SwapMemory_min}
	Ab_SwapMemory = []float64{norm_data[0].SwapMemory_max, norm_data[0].SwapMemory_avg, norm_data[0].SwapMemory_min}
	Aw_DiskIOCounterBusyTime = []float64{norm_data[0].DiskIOCounterBusyTimeMax, norm_data[0].DiskIOCounterBusyTimeAVG, norm_data[0].DiskIOCounterBusyTimeMin}
	Ab_DiskIOCounterBusyTime = []float64{norm_data[0].DiskIOCounterBusyTimeMax, norm_data[0].DiskIOCounterBusyTimeAVG, norm_data[0].DiskIOCounterBusyTimeMin}
	Aw_NetIOCounterDropin = []float64{norm_data[0].NetIOCounterDropinMax, norm_data[0].NetIOCounterDropinAVG, norm_data[0].NetIOCounterDropinMin}
	Ab_NetIOCounterDropin = []float64{norm_data[0].NetIOCounterDropinMax, norm_data[0].NetIOCounterDropinAVG, norm_data[0].NetIOCounterDropinMin}
	Aw_NetIOCounterDropout = []float64{norm_data[0].NetIOCounterDropoutMax, norm_data[0].NetIOCounterDropoutAVG, norm_data[0].NetIOCounterDropoutMin}
	Ab_NetIOCounterDropout = []float64{norm_data[0].NetIOCounterDropoutMax, norm_data[0].NetIOCounterDropoutAVG, norm_data[0].NetIOCounterDropoutMin}

	for _, values := range norm_data {
		if Aw_CPUUsage_num < values.CPUUsageMax {
			Aw_CPUUsage_num = values.CPUUsageMax
			Aw_CPUUsage = []float64{values.CPUUsageMax, values.CPUUsageAVG, values.CPUUsageMin}
		}
		if Ab_CPUUsage_num > values.CPUUsageMin {
			Ab_CPUUsage_num = values.CPUUsageMin
			Ab_CPUUsage = []float64{values.CPUUsageMax, values.CPUUsageAVG, values.CPUUsageMin}
		}
		if Aw_MemoryUsage_num < values.MemoryUsageMax {
			Aw_MemoryUsage_num = values.MemoryUsageMax
			Aw_MemoryUsage = []float64{values.MemoryUsageMax, values.MemoryUsageAVG, values.MemoryUsageMin}
		}
		if Ab_MemoryUsage_num > values.MemoryUsageMin {
			Ab_MemoryUsage_num = values.MemoryUsageMin
			Ab_MemoryUsage = []float64{values.MemoryUsageMax, values.MemoryUsageAVG, values.MemoryUsageMin}
		}
		if Aw_TailLatency_num < values.TailLatencyMax {
			Aw_TailLatency_num = values.TailLatencyMax
			Aw_TailLatency = []float64{values.TailLatencyMax, values.TailLatencyAVG, values.TailLatencyMin}
		}
		if Ab_TailLatency_num > values.TailLatencyMin {
			Ab_TailLatency_num = values.TailLatencyMin
			Ab_TailLatency = []float64{values.TailLatencyMax, values.TailLatencyAVG, values.TailLatencyMin}
		}
		if Aw_DiskFree_num > values.DiskFreeMin {
			Aw_DiskFree_num = values.DiskFreeMin
			Aw_DiskFree = []float64{values.DiskFreeMax, values.DiskFreeAVG, values.DiskFreeMin}
		}
		if Ab_DiskFree_num < values.DiskFreeMax {
			Ab_DiskFree_num = values.DiskFreeMax
			Ab_DiskFree = []float64{values.DiskFreeMin, values.DiskFreeAVG, values.DiskFreeMax}
		}
		if Aw_SwapMemory_num < values.SwapMemory_max {

			Aw_SwapMemory_num = values.SwapMemory_max
			Aw_SwapMemory = []float64{values.SwapMemory_max, values.SwapMemory_avg, values.SwapMemory_min}
		}

		if Ab_SwapMemory_num > values.SwapMemory_min {
			Ab_SwapMemory_num = values.SwapMemory_min
			Ab_SwapMemory = []float64{values.SwapMemory_max, values.SwapMemory_avg, values.SwapMemory_min}
		}
		if Aw_DiskIOCounterBusyTime_num < values.DiskIOCounterBusyTimeMax {
			Aw_DiskIOCounterBusyTime_num = values.DiskIOCounterBusyTimeMax
			Aw_DiskIOCounterBusyTime = []float64{values.DiskIOCounterBusyTimeMax, values.DiskIOCounterBusyTimeAVG, values.DiskIOCounterBusyTimeMin}
		}
		if Ab_DiskIOCounterBusyTime_num > values.DiskIOCounterBusyTimeMin {
			Ab_DiskIOCounterBusyTime_num = values.DiskIOCounterBusyTimeMin
			Ab_DiskIOCounterBusyTime = []float64{values.DiskIOCounterBusyTimeMax, values.DiskIOCounterBusyTimeAVG, values.DiskIOCounterBusyTimeMin}
		}
		if Aw_NetIOCounterDropin_num < values.NetIOCounterDropinMax {
			Aw_NetIOCounterDropin_num = values.NetIOCounterDropinMax
			Aw_NetIOCounterDropin = []float64{values.NetIOCounterDropinMax, values.NetIOCounterDropinAVG, values.NetIOCounterDropinMin}
		}
		if Ab_NetIOCounterDropin_num > values.NetIOCounterDropinMin {
			Ab_NetIOCounterDropin_num = values.NetIOCounterDropinMin
			Ab_NetIOCounterDropin = []float64{values.NetIOCounterDropinMax, values.NetIOCounterDropinAVG, values.NetIOCounterDropinMin}
		}
		if Aw_NetIOCounterDropout_num < values.NetIOCounterDropoutMax {
			Aw_NetIOCounterDropout_num = values.NetIOCounterDropoutMax
			Aw_NetIOCounterDropout = []float64{values.NetIOCounterDropoutMax, values.NetIOCounterDropoutAVG, values.NetIOCounterDropoutMin}
		}
		if Ab_NetIOCounterDropout_num > values.NetIOCounterDropoutMin {
			Ab_NetIOCounterDropout_num = values.NetIOCounterDropoutMin
			Ab_NetIOCounterDropout = []float64{values.NetIOCounterDropoutMax, values.NetIOCounterDropoutAVG, values.NetIOCounterDropoutMin}
		}

	}

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
