package sync

import "container/list"

type Keyer interface {
	GetKey() int64
}

type MapList struct {
	dataMap  map[int64]*list.Element
	dataList *list.List
}

func NewMapList() *MapList {
	return &MapList{
		dataMap:  make(map[int64]*list.Element),
		dataList: list.New(),
	}
}

func (mapList *MapList) Exists(data Keyer) bool {
	_, exists := mapList.dataMap[data.GetKey()]
	return exists
}

func (mapList *MapList) Push(data Keyer) bool {
	if mapList.Exists(data) {
		return false
	}
	elem := mapList.dataList.PushBack(data)
	mapList.dataMap[data.GetKey()] = elem
	return true
}

func (mapList *MapList) Remove(data Keyer) {
	if !mapList.Exists(data) {
		return
	}
	mapList.dataList.Remove(mapList.dataMap[data.GetKey()])
	delete(mapList.dataMap, data.GetKey())
}

func (mapList *MapList) Walk(cb func(data Keyer)) {
	for elem := mapList.dataList.Front(); elem != nil; elem = elem.Next() {
		cb(elem.Value.(Keyer))
	}
}

func (mapList *MapList) RemoveBefore(k int64) {
	var next *list.Element
	for elem := mapList.dataList.Front(); elem != nil; elem = next {
		if elem.Value.(Keyer).GetKey() <= k {
			next = elem.Next()
			mapList.Remove(elem.Value.(Keyer))
		} else {
			break
		}
	}
}

func (mapList *MapList) Size () int {
	return mapList.dataList.Len()
}

func (mapList *MapList) GetDataList () *list.List {
	return mapList.dataList
}