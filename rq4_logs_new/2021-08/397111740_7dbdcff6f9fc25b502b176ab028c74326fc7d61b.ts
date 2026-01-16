const Convert = require('xml-js');

export function extract_process_from_xml(xml: string): any {
  const options = {
    compact: true,
    alwaysArray: true,
    ignoreDeclaration: true,
  };

  const js = Convert.xml2js(xml, options);

  return js['bpmn2:definitions'][0]['bpmn2:process'][0];
}

// id 로 요소 찾기
export function find_element_by_id(js: any, id: string): any {
  for (let [key, value] of Object.entries<Array<any>>(js)) {
    for (let v of value) {
      let cur_id = v['_attributes']['id'];
      if (cur_id == id) {
        let ans = Object();
        ans[key] = value;
        return ans;
      }
    }
  }
  return null;
}

export function find_element_by_name(js: any, name: string): any {
  for (let [key, value] of Object.entries<Array<any>>(js)) {
    if (key == name) {
      let ans = Object();
      ans[key] = value;
      return ans;
    }
  }
  return null;
}

// const result = extract_process_from_xml(test_xml);
// console.log(result);
// console.log(result['bpmn2:sequenceFlow'][0]['_attributes']['id']);
// console.log(result['bpmn2:exclusiveGateway'][0]['bpmn2:incoming'][0]);
// console.log(find_element_by_id(result, 'Flow_0qdz5ko'));
// console.log(find_element_by_name(result, 'bpmn2:startEvent'));
// console.log(find_element_by_name(result, 'bpmn2:sequenceFlow'));