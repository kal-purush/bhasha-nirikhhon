using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using Microsoft.EntityFrameworkCore;
using ProjectPRN.Models;

namespace ProjectPRN
{
    /// <summary>
    /// Interaction logic for CategoryManagementWindow.xaml
    /// </summary>
    public partial class CategoryManagementWindow : Window
    {
        private readonly ProjectPrnContext context;
        private Category selectedCategory;
        private ObservableCollection<Category> categories;
        public CategoryManagementWindow()
        {
            context = new ProjectPrnContext();
            categories = new ObservableCollection<Category>();
            InitializeComponent();
        }

        private void dgvCategories_Loaded(object sender, RoutedEventArgs e)
        {
            LoadCategories();
        }
        private void LoadCategories()
        {
            try
            {
                categories.Clear();
                foreach (var category in context.Categories.ToList())
                {
                    categories.Add(category);
                }
                dgvCategories.ItemsSource = categories;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Lỗi tải categories: {ex.Message}", "Lỗi", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        private void dgvCategories_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                selectedCategory = dgvCategories.SelectedItem as Category;
                if (selectedCategory != null)
                {
                    txtCategoryId.Text = selectedCategory.CategoryId.ToString();
                    txtCategoryName.Text = selectedCategory.CategoryName;
                    txtCategoryDescription.Text = selectedCategory.CategoryDescription;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Lỗi : {ex.Message}", "Lỗi", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void btnSave_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                
                // Kiểm tra nhập liệu
                if (string.IsNullOrWhiteSpace(txtCategoryName.Text) ||
                    string.IsNullOrWhiteSpace(txtCategoryDescription.Text))
                {
                    MessageBox.Show("Vui lòng điền đầy đủ thông tin!", "Thông báo", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }

                // Kiểm tra trùng tên category
                string newCategoryName = txtCategoryName.Text.Trim();
                if (selectedCategory == null || selectedCategory.CategoryName != newCategoryName)
                {
                    if (IsExistCategoryName(newCategoryName))
                    {
                        MessageBox.Show("Tên danh mục đã tồn tại!", "Lỗi", MessageBoxButton.OK, MessageBoxImage.Warning);
                        return;
                    }
                }

                Category category = selectedCategory == null ? new Category() : context.Categories.FirstOrDefault(p => p.CategoryId == selectedCategory.CategoryId);

                if (category == null) // Trường hợp không tìm thấy sản phẩm trong DB
                {
                    MessageBox.Show("Danh mục không tồn tại!", "Lỗi", MessageBoxButton.OK, MessageBoxImage.Error);
                    return;
                }
                // Cập nhật dữ liệu
                category.CategoryName = newCategoryName;
                category.CategoryDescription = txtCategoryDescription.Text.Trim();

                if (selectedCategory == null) // Nếu đang thêm mới
                {
                    context.Categories.Add(category);
                }

                // Lưu thay đổi vào DB
                context.SaveChanges();

                // Cập nhật danh sách sản phẩm
                LoadCategories();

                // Hiển thị thông báo
                MessageBox.Show(category.CategoryId == 0 ? "Thêm danh mục thành công!" : "Cập nhật danh mục thành công!",
                                "Thông báo", MessageBoxButton.OK, MessageBoxImage.Information);

                // Reset form
                ClearForm();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Lỗi khi lưu sản phẩm: {ex.Message}", "Lỗi", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        private void ClearForm()
        {
            txtCategoryId.Text = string.Empty;
            txtCategoryName.Text = string.Empty;
            txtCategoryDescription.Text = string.Empty;
            btnSave.Content = "Lưu";
        }
        private bool IsExistCategoryName(string categoryName)
        {
            return context.Categories
        .Any(x => x.CategoryName.Equals(categoryName));
        }
        private void btnDelete_Click(object sender, RoutedEventArgs e)
        {
            if (selectedCategory == null)
            {
                MessageBox.Show("Vui lòng chọn Danh mục cần xóa!", "Lỗi", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            // Hiển thị hộp thoại xác nhận
            MessageBoxResult result = MessageBox.Show(
                "Bạn có chắc chắn muốn xóa danh mục này?",
                "Xác nhận xóa",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);

            if (result == MessageBoxResult.Yes)
            {
                try
                {
                    Category? cat = context.Categories.Find(selectedCategory.CategoryId);
                    if (cat != null)
                    {
                        context.Categories.Remove(cat);
                        context.SaveChanges();
                        MessageBox.Show("Xóa thành công!", "Thông báo", MessageBoxButton.OK, MessageBoxImage.Information);
                        LoadCategories(); // Cập nhật danh sách sau khi xóa
                    }
                    else
                    {
                        MessageBox.Show("Không tìm thấy danh mục để xóa!", "Lỗi", MessageBoxButton.OK, MessageBoxImage.Warning);
                    }
                }
                catch (Exception ex)
                {
                    MessageBox.Show("Lỗi khi xóa danh mục: " + ex.Message, "Lỗi", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private void btnClear_Click(object sender, RoutedEventArgs e)
        {
            if (selectedCategory != null)
            {
                // Nếu selectedProduct đang được theo dõi trong DbContext, detach nó
                context.Entry(selectedCategory).State = EntityState.Detached;
                selectedCategory = null;
            }
            txtCategoryId.Text = string.Empty;
            txtCategoryName.Text = string.Empty;
            txtCategoryDescription.Text = string.Empty;
        }

        private void btnProduct_Click(object sender, RoutedEventArgs e)
        {
            ProductManagementWindow productManagementWindow = new ProductManagementWindow();
            productManagementWindow.Show();
            this.Close();
        }
    }
}