import { Button, Form, Select, Input, InputNumber, Space } from 'antd';
export default function EditItem(props) {
    const [form] = Form.useForm();
    return (
        <Form 
            layout="vertical"
            onFinish={props.onItemAdded}
        >
            <Form.Item
                name="type"
                label="ชนิด :"
                rules={[{ required: true }]}
            >
                <Select
                    allowClear
                    style={{ width: "100px" }}
                    options={[
                        {
                            value: 'income',
                            label: 'รายรับ',
                        },
                        {
                            value: 'expense',
                            label: 'รายจ่าย',
                        },
                    ]}
                />
            </Form.Item>
            <Form.Item
                name="amount"
                label="จำนวนเงิน :"
                rules={[{ required: true }]}>
                <InputNumber placeholder="จำนวนเงิน" />
            </Form.Item>
            <Form.Item
                name="note"
                label="หมายเหตุ :"
                rules={[{ required: true }]}>
                <Input placeholder="Note" />
            </Form.Item>
            <Form.Item>
                <Space>
                    <Button htmlType="button" onClick={() => form.resetFields()}>
                        Cancel
                    </Button>
                    <Button type="primary" htmlType="submit">
                        Edit
                    </Button>
                </Space>
            </Form.Item>
        </Form>
    )
}