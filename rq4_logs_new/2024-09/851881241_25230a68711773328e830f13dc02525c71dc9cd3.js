import React, { useState } from 'react';
import { ScrollView, View } from 'react-native';
import { Layout, Button, Text } from '@ui-kitten/components';
import DynamicForm from '../components/DynamicForm';

const FormFillerScreen = ({ route }) => {
  const { form } = route.params; // Datos del formulario pasado desde el FormSelector
  const [responses, setResponses] = useState({}); // Estado para las respuestas

  // Almacenar respuestas de DynamicForm
    const handleFormChange = (field, value) => {
        setResponses((prev) => ({ ...prev, [field]: value }));
    };

    const handleSubmit = () => {
        console.log('Respuestas del formulario:', responses);
        // Aquí puedes enviar las respuestas o hacer algo con ellas
    };

    return (
        <Layout style={{ flex: 1, padding: 16 }}>
        <ScrollView>
            <Text category="h5">{form["nombre formulario"]}</Text>
            
            <View style={{ marginTop: 20 }}>
            <DynamicForm formData={form} onFormChange={handleFormChange} />
            </View>
            
            <View style={{ marginTop: 20 }}>
            </View>
        </ScrollView>
        </Layout>
    );
};

export default FormFillerScreen;