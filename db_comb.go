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

// 数据操作 //////////////////////////////////
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
		Filted: d.Filted,
	}
}

// 按split统计个数 新字段名=老字段名前面+下划线,后面加scount
func (d *Comb) CountSplit(fkey, newKey string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for i, v := range d.Data {
		if _, ok := v[fkey]; !ok {
			continue
		}
		fval := fmt.Sprintf("%v", v[fkey])
		spl := strings.Split(fval, ",")
		d.Data[i][newKey] = len(spl)
		tempData = append(tempData, v)
	}
	return &Comb{
		Data:   tempData,
		Filted: d.Filted,
	}
}

// 按range统计个数 新字段=老子段名前面+下划线,后面加rcount
func (d *Comb) CountRange(fkey, newKey string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for i, v := range d.Data {
		if _, ok := v[fkey]; !ok {
			continue
		}
		fval := fmt.Sprintf("%v", v[fkey])
		spl := strings.Split(fval, ",")
		var total int64
		for _, vv := range spl {
			spr := strings.Split(vv, "-")
			beginI64, _ := strconv.ParseInt(spr[0], 10, 64)
			endInt64, _ := strconv.ParseInt(spr[1], 10, 64)
			if endInt64 > 0 {
				total += (endInt64 - beginI64) + 1
			}
		}

		d.Data[i][newKey] = total
		tempData = append(tempData, v)
	}
	return &Comb{
		Data:   tempData,
		Filted: d.Filted,
	}
}

// 多个字段相加计算结果
func (d *Comb) CountSum(newKey string, sumKey ...string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for i, v := range d.Data {
		var sumRes float64
		for _, skey := range sumKey {
			if sval, ok := v[skey]; ok {
				sF64, _ := strconv.ParseFloat(fmt.Sprintf("%v", sval), 64)
				sumRes += sF64
			}
		}
		d.Data[i][newKey] = sumRes
		tempData = append(tempData, v)
	}
	return &Comb{
		Data:   tempData,
		Filted: d.Filted,
	}
}

// 关联字段
func (d *Comb) ConcatSeparator(separator, newKey string, fields ...string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	for i, v := range d.Data {
		newValue := make([]string, 0)
		for _, sField := range fields {
			if sval, ok := v[sField]; ok {
				newValue = append(newValue, fmt.Sprintf("%v", sval))
			}
		}
		d.Data[i][newKey] = strings.Join(newValue, separator)
		tempData = append(tempData, v)
	}
	return &Comb{
		Data:   tempData,
		Filted: d.Filted,
	}
}

// 数据关联 ///////////////////////////////////////
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

// 左关联指定字段
func (d *Comb) LeftJoinWithFields(comp *Comb, fieldLeft, fieldRight string, fields ...string) *Comb {
	// 如果要组合的数据为空
	if len(comp.Data) == 0 {
		return d
	}
	for i, v := range d.Data {
		d.Data[i]["__"] = "0"
		v1 := fmt.Sprintf("%s", v[fieldLeft])
		for _, vv := range comp.Data {
			if v2, ok := vv[fieldRight]; ok && fmt.Sprintf("%s", v2) == v1 {
				d.Data[i]["__"] = "1"
				// 关联字段相同,合并map
				for f, fv := range vv {
					if arrayContain(f, fields) {
						d.Data[i][f] = fv
					}
				}
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

// 内关联指定字段
func (d *Comb) InnerJoinWithFields(comp *Comb, fieldLeft, fieldRight string, fields ...string) *Comb {
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
					if arrayContain(f, fields) {
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

// 排除连接, 左侧数据中不能有右侧数据同Field数据
func (d *Comb) ExcludeJoin(comp *Comb, fieldLeft, fieldRight string) *Comb {
	tempData := make([]map[string]interface{}, 0)
	if len(comp.Data) == 0 {
		return d
	}

	// 先把右侧数据做成MAP
	tempMap := make(map[string]string, 0)
	for _, v := range comp.Data {
		if rVal, ok := v[fieldRight]; ok {
			tempMap[fmt.Sprintf("%v", rVal)] = ""
		}
	}

	// 左侧数据如果等于右侧数据,排除掉
	for _, v := range d.Data {
		if lVal, ok := v[fieldLeft]; ok {
			if _, ook := tempMap[fmt.Sprintf("%v", lVal)]; !ook {
				tempData = append(tempData, v)
			}
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

// 合并数据-> 要求两边数据字段一致(不一致也没事儿)
func (d *Comb) UnionAll(comp *Comb) *Comb {
	tempData := make([]map[string]interface{}, 0)
	tempData = append(tempData, d.Data...)
	tempData = append(tempData, comp.Data...)
	return &Comb{
		Data:   tempData,
		Filted: d.Filted,
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

// 生成tree数据
// 列表转Tree
//
//	export const listToTree = (id, arr, pidName, keyName) => {
//	    let array = []
//	    arr.forEach(item => {
//	        if (item[pidName] === id) {
//	            item.children = listToTree(item[keyName], arr,pidName,keyName) // 接收子节点
//	            array.push(item)
//	        };
//	    })
//	    return array
//	}
func (d *Comb) ToTree(startID, pidName, keyName string) []map[string]interface{} {
	tempData := make([]map[string]interface{}, 0)
	for _, v := range d.Data {
		if fmt.Sprintf("%v", v[pidName]) == startID {
			v["children"] = d.ToTree(fmt.Sprintf("%v", v[keyName]), pidName, keyName)
			tempData = append(tempData, v)
		}
	}
	return tempData
}

// ///////all_sum
func (d *Comb) AllSum(fkey string) (res int64) {
	for _, v := range d.Data {
		if fv, ok := v[fkey]; ok {
			fvalInt64, _ := strconv.ParseInt(fmt.Sprintf("%v", fv), 10, 64)
			res += fvalInt64
		}
	}
	return
}
