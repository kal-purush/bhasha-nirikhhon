#�������
def main():
  arr1 = [5,3,7,5,4,7,6,12,24,51,623]
  arr2 = [4,5,2,6,121,21,4421,22]
  string1 = "HALO GAIS"
  result = find_min_in_arr(arr1)
  ivan = {
    "name": "ivan",
    "age": 34,
    "children": [{
      "name": "vasja",
      "age": 19,
    },{
      "name": "petja",
      "age": 19,
    }],
  }
  darja = {
    "name": "darja",
    "age": 41,
    "children": [{
      "name": "kirill",
      "age": 21,
    }, {
      "name": "pavel",
      "age": 19,
    }]
  }
  emps = [ivan, darja]
  
  print("min_value 1 = " + str(result))
  print("min_value 2 =",  find_min_in_arr(arr2))
  print("average_value 1 = ", find_average_value(arr1))
  print("average_value 2 = ", find_average_value(arr2))
  print("Replaced string = ", string_replace(string1))
  children_check(emps)
  
def find_min_in_arr(arr):
  min_value = arr[0]
  for value in arr:
    if min_value >= value:
      min_value = value
      
  return min_value
#print("min value for array 1 = ", find_min_in_arr(arr1))


def find_average_value(arr):
  summ = 0
  for value in arr:
    summ += value
  average_value = summ/len(arr)
  return average_value
#print("average_value for array 1 = ", find_average_value(arr1))

#print("min value for array 2 = ", find_min_in_arr(arr2))
#print("average_value for array 2 = ", find_average_value(arr2))


#������


def string_replace(string_sample):
  string_replaced = ""
  lenght = len(string_sample)
  while lenght != 0:
    lenght -= 1
    string_replaced = string_replaced + string_sample[lenght]
  return string_replaced
  
def children_check(employees):
  for empl in employees:
    for child in empl.get('children', []):
      if child['age'] >= 18:
        print(empl['name'])
        break
  return 0



main()
  