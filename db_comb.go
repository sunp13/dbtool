package dbtool

import (
	"fmt"
	"strconv"
	"strings"
)

// 数据梳理
type Comb struct {
	Data   []map[string]interface{}
	Filted bool
}

// 创建一个组合器
func NewComb(data []map[string]interface{}) *Comb {
	return &Comb{
		Data: data,
	}
}

// 过滤相等
func (d *Comb) FilterMust(fkey, fval string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok && fmt.Sprintf("%v", val) == fval {
			tempData = append(tempData, v)
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 过滤like
func (d *Comb) FilterLike(fkey, fval string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok && strings.Contains(fmt.Sprintf("%v", val), fval) {
			tempData = append(tempData, v)
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 数字 大于
func (d *Comb) FilterMoreThanNumber(fkey string, fval string) *Comb {
	fvalF64, _ := strconv.ParseFloat(fval, 64)
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok {
			str := fmt.Sprintf("%v", val)
			strF64, _ := strconv.ParseFloat(str, 64)

			if strF64 > fvalF64 {
				tempData = append(tempData, v)
			}
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 数字 大于等于
func (d *Comb) FilterMoreEqualNumber(fkey string, fval string) *Comb {
	fvalF64, _ := strconv.ParseFloat(fval, 64)
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok {
			str := fmt.Sprintf("%v", val)
			strF64, _ := strconv.ParseFloat(str, 64)

			if strF64 >= fvalF64 {
				tempData = append(tempData, v)
			}
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 数字 小于
func (d *Comb) FilterLessThanNumber(fkey string, fval string) *Comb {
	fvalF64, _ := strconv.ParseFloat(fval, 64)
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok {
			str := fmt.Sprintf("%v", val)
			strF64, _ := strconv.ParseFloat(str, 64)

			if strF64 < fvalF64 {
				tempData = append(tempData, v)
			}
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 数字 小于等于
func (d *Comb) FilterLessEqualNumber(fkey string, fval string) *Comb {
	fvalF64, _ := strconv.ParseFloat(fval, 64)
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok {
			str := fmt.Sprintf("%v", val)
			strF64, _ := strconv.ParseFloat(str, 64)

			if strF64 <= fvalF64 {
				tempData = append(tempData, v)
			}
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 过滤不等
func (d *Comb) FilterMustNot(fkey, fval string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok && fmt.Sprintf("%v", val) == fval {
			continue
		} else {
			tempData = append(tempData, v)
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 字符串 在数组中, 要求数据库中的字段逗号分开
func (d *Comb) FilterInList(fkey string, fval string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok {
			// 数据库中的数据
			str := fmt.Sprintf("%v", val)
			spl := strings.Split(str, ",")
			for _, vv := range spl {
				if vv == fval {
					tempData = append(tempData, v)
					break
				}
			}
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 字符串在两个字段区间内是否存在-包含begin,end
func (d *Comb) FilterBetween(fkeyBegin, fkeyEnd string, fval string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if _, ok := v[fkeyBegin]; !ok {
			continue
		}

		if _, ok := v[fkeyEnd]; !ok {
			continue
		}

		beginF64, _ := strconv.ParseFloat(fmt.Sprintf("%v", v[fkeyBegin]), 64)
		endF64, _ := strconv.ParseFloat(fmt.Sprintf("%v", v[fkeyEnd]), 64)
		fvalF64, _ := strconv.ParseFloat(fval, 64)

		if (fvalF64 >= beginF64) && (fvalF64 <= endF64) {
			tempData = append(tempData, v)
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// 比对一个字段中的最小值,最大值, 逗号分隔的字段
func (d *Comb) FilterRangeSplit(rangeKey string, fval string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if _, ok := v[rangeKey]; !ok {
			continue
		}
		fieldValue := fmt.Sprintf("%v", v[rangeKey])
		fieldSpl := strings.Split(fieldValue, ",")
		if len(fieldSpl) != 2 {
			// 如果字段不是2段
			continue
		}
		beginF64, _ := strconv.ParseFloat(fieldSpl[0], 64)
		endF64, _ := strconv.ParseFloat(fieldSpl[1], 64)
		fvalF64, _ := strconv.ParseFloat(fval, 64)
		if fvalF64 >= beginF64 && fvalF64 <= endF64 {
			tempData = append(tempData, v)
		}
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// //////////////////////////////////
// 按字段排重
func (d *Comb) UniqSort(fkey string) *Comb {
	sortMap := make(map[string]map[string]interface{}, 0)
	for _, v := range d.Data {
		if val, ok := v[fkey]; ok {
			sortMap[fmt.Sprintf("%v", val)] = v
		}
	}
	tempData := make([]map[string]interface{}, 0)
	for _, v := range sortMap {
		tempData = append(tempData, v)
	}
	return &Comb{
		Data:   tempData,
		Filted: true,
	}
}

// ///////////////////////////////////////
// 左关联
func (d *Comb) LeftJoin(comp *Comb, fieldLeft, fieldRight string) *Comb {
	// 如果要组合的数据为空
	if len(comp.Data) == 0 {
		return d
	}

	// 要关联的字段
	joinFields := make(map[string]struct{}, 0)
	for k := range comp.Data[0] {
		if k != fieldRight {
			joinFields[k] = struct{}{}
		}
	}

	for i, v := range d.Data {
		d.Data[i]["__"] = "0"
		v1 := fmt.Sprintf("%s", v[fieldLeft])
		for _, vv := range comp.Data {
			if v2, ok := vv[fieldRight]; ok && fmt.Sprintf("%s", v2) == v1 {
				d.Data[i]["__"] = "1"
				// 关联字段相同,合并map
				for f, fv := range vv {
					if f == fieldRight {
						continue
					} else {
						d.Data[i][f] = fv
					}
				}
			}
		}
		if fmt.Sprintf("%s", d.Data[i]["__"]) == "0" {
			for k := range joinFields {
				d.Data[i][k] = ""
			}
		}
	}
	return d
}

// 内关联
func (d *Comb) InnerJoin(comp *Comb, fieldLeft, fieldRight string) *Comb {
	tempData := make([]map[string]interface{}, 0)

	if len(d.Data) == 0 || len(comp.Data) == 0 {
		return &Comb{
			Data: tempData,
		}
	}

	for i, v := range d.Data {
		d.Data[i]["__"] = "0"
		v1 := fmt.Sprintf("%s", v[fieldLeft])
		for _, vv := range comp.Data {
			if v2, ok := vv[fieldRight]; ok && fmt.Sprintf("%s", v2) == v1 {
				d.Data[i]["__"] = "1"
				// 关联字段相同,合并map
				for f, fv := range vv {
					if f == fieldRight {
						continue
					} else {
						d.Data[i][f] = fv
					}
				}
			}
		}
		if fmt.Sprintf("%s", d.Data[i]["__"]) == "1" {
			tempData = append(tempData, v)
		}
	}
	return &Comb{
		Data: tempData,
	}
}

// 过滤关联
func (d *Comb) FilterJoin(comp *Comb, fieldLeft, fieldRight string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	if len(d.Data) == 0 || len(comp.Data) == 0 {
		return &Comb{
			Data: tempData,
		}
	}

	for i, v := range d.Data {
		d.Data[i]["__"] = "0"
		v1 := fmt.Sprintf("%s", v[fieldLeft])
		for _, vv := range comp.Data {
			if v2, ok := vv[fieldRight]; ok && fmt.Sprintf("%s", v2) == v1 {
				d.Data[i]["__"] = "1"
				tempData = append(tempData, v)
			}
		}
		// if fmt.Sprintf("%s", d.Data[i]["__"]) == "1" {
		// 	tempData = append(tempData, v)
		// }
	}
	return &Comb{
		Data: tempData,
	}
}

// 自动组合数据, // 如果comb2 isFilted, innerJoin, 否则 leftjoin
func (d *Comb) CombAutoJoin(comb2 *Comb, fleft, fright string) *Comb {
	if comb2.Filted {
		return d.InnerJoin(comb2, fleft, fright)
	} else {
		return d.LeftJoin(comb2, fleft, fright)
	}
}
