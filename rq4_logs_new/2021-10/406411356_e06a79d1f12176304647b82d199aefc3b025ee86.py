# Создайте класс Editor, который содержит методы view_document и edit_document. Пусть метод
# edit_document выводит на экран информацию о том, что редактирование документов недоступно для
# бесплатной версии. Создайте подкласс ProEditor, в котором данный метод будет переопределён.
# Введите с клавиатуры лицензионный ключ и, если он корректный, создайте экземпляр класса ProEditor,
# иначе Editor. Вызовите методы просмотра и редактирования документов.


class Editor:
    text_document = ''

    def view_document(self):
        print(self.text_document)

    def edit_document(self):
        print("The document editing function is not available for the free version of this program.")
        print(self.text_document)


class ProEditor(Editor):

    def edit_document(self):
        print(self.text_document)
        edit_text = input("You can edd text from this document: ")
        self.text_document = self.text_document + edit_text
        print(self.text_document)


class main:

    __license_key = 'ZAQ12wsx'
    input_key = input("Input licence key from use version ProEditor or use FreeVersion: ")
    if input_key == __license_key:
        pro_version = ProEditor()
        pro_version.edit_document()
    else:
        free_version = Editor()
        free_version.edit_document()

    

if __name__ == '__main__':
    main()
    