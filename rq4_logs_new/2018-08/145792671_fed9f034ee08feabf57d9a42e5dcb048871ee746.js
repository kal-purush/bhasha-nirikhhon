import React, { Component } from 'react';
import './style.scss';
import { Upload, Icon, Button } from 'antd';
import { Field } from 'react-formutil';

class PicturesWall extends Component {
    state = {
        fileList: []
    };

    render() {
        const { fileList } = this.state;
        const uploadButton = (
            <div>
                <Icon type="plus" />
            </div>
        );

        return (
            <Field
                name="avatar_img"
                required
                $validators={{
                    required: value => !!value || '请选择您的头像'
                }}>
                {$props => {
                    const handleChange = ({ fileList }) => {
                        this.setState({ fileList });
                        if (fileList[0].response) {
                            $props.$render(fileList[0].response.data.url);
                        }
                    };

                    const handleDelete = () => this.setState({ fileList: [] });
                    return (
                        <div className="clearfix upload">
                            <Upload
                                action="http://39.106.221.165/app/u-image/by-company/890D05A417BE05A0/1"
                                accept="image/*"
                                name="image"
                                listType="picture-card"
                                fileList={fileList}
                                onChange={handleChange}>
                                {fileList.length > 0 ? null : uploadButton}
                            </Upload>
                            {fileList.length > 0 && (
                                <Button type="primary" onClick={handleDelete}>
                                    删除
                                </Button>
                            )}
                        </div>
                    );
                }}
            </Field>
        );
    }
}

export default PicturesWall;