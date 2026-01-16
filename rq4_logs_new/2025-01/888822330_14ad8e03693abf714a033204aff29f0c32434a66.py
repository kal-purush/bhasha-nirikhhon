import google.generativeai as genai

# Configure a chave da API
genai.configure(api_key="AIzaSyDTG1Qi4Shsp0H2VFG0G5emNPFiNVxBcYI")

# Usar um modelo generativo, por exemplo, "gemini-pro" ou outro modelo disponível
model = genai.GenerativeModel("gemini-pro")

# Solicitar um conteúdo (você pode alterar o texto conforme desejar)
response = model.generate_content("o que é a água?")

# Exibir o conteúdo gerado
print(response.text)