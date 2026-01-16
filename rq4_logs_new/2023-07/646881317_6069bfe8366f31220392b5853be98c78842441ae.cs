using _1.DATA.Model;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data.Extensions
{
    public static class ModelBuilderExtensions
    {
        public static void Seed(this ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SanPhamChiTiet>().HasData(
                new SanPhamChiTiet()
                {
                    Id = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSP = new Guid("01BF8A6C-D89A-4DB2-9068-AF277BE488AE"),
                    IdMauSac = new Guid("7A6C0C50-FB67-44EA-9C62-AD0E0F67AB3C"),
                    TenSPChiTiet = "V1",
                    MaSPChiTiet = "SP1",
                    GiaBan = 100000,
                    TrangThai = 1,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SanPhamChiTiet()
                {
                    Id = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSP = new Guid("9A1B3786-D0D0-402A-BFD8-10DB04F8E6ED"),
                    IdMauSac = new Guid("5E5F7D51-044F-4928-9AAC-4A496C629231"),
                    TenSPChiTiet = "V2",
                    MaSPChiTiet = "SP2",
                    GiaBan = 200000,
                    TrangThai = 1,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SanPhamChiTiet()
                {
                    Id = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSP = new Guid("01BF8A6C-D89A-4DB2-9068-AF277BE488AE"),
                    IdMauSac = new Guid("8D364A55-877F-45A5-9408-66AFB8AB873F"),
                    TenSPChiTiet = "V1",
                    MaSPChiTiet = "SP1",
                    GiaBan = 100000,
                    TrangThai = 1,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,

                },
                new SanPhamChiTiet()
                {
                    Id = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSP = new Guid("9A1B3786-D0D0-402A-BFD8-10DB04F8E6ED"),
                    IdMauSac = new Guid("CFA013B0-34DC-44D5-A471-7C49A9AC167D"),
                    TenSPChiTiet = "V2",
                    MaSPChiTiet = "SP2",
                    GiaBan = 200000,
                    TrangThai = 1,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                }
                );
            modelBuilder.Entity<Size>().HasData(
                new Size()
                {
                    Id = new Guid("CD27319D-468D-4189-99A4-B2DA273492A3"),
                    KichCo = 36,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new Size()
                {
                    Id = new Guid("D43ED236-C9CF-4659-BA0D-F834BB316D39"),
                    KichCo = 37,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new Size()
                {
                    Id = new Guid("D6DC21E2-F1BE-48EC-8F30-BD6BBC73DAEC"),
                    KichCo = 38,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new Size()
                {
                    Id = new Guid("9297A9B2-5C69-4B52-AF99-57BBF137DCF1"),
                    KichCo = 39,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new Size()
                {
                    Id = new Guid("3A218DE7-0C55-4B70-A0D8-823195532B66"),
                    KichCo = 40,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new Size()
                {
                    Id = new Guid("2D53402B-696D-4E6E-B95E-FB5F2A99C014"),
                    KichCo = 41,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new Size()
                {
                    Id = new Guid("560CD8D3-52CC-4204-9145-0A142CDAC1FD"),
                    KichCo = 42,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new Size()
                {
                    Id = new Guid("9B9A3A86-196E-4FB2-9300-A64784962D59"),
                    KichCo = 43,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                }
                );
            modelBuilder.Entity<SizeSanPham>().HasData(

                new SizeSanPham()
                {
                    Id = new Guid("5EE0FA3D-9AB6-490A-83CA-01021103412D"),
                    IdSanPhamChiTiet = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSize = new Guid("CD27319D-468D-4189-99A4-B2DA273492A3"),
                    SoLuong = 10,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("7378B6EC-8274-4186-8855-0CAFC1CE5881"),
                    IdSanPhamChiTiet = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSize = new Guid("D43ED236-C9CF-4659-BA0D-F834BB316D39"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("D09FD8B7-8263-4014-91D8-63EA9470A8DE"),
                    IdSanPhamChiTiet = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSize = new Guid("D6DC21E2-F1BE-48EC-8F30-BD6BBC73DAEC"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("5885A945-5722-4177-8FA7-D26B0DACF980"),
                    IdSanPhamChiTiet = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSize = new Guid("9297A9B2-5C69-4B52-AF99-57BBF137DCF1"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("0515437F-C4AB-4956-8B65-4F7D183B1620"),
                    IdSanPhamChiTiet = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSize = new Guid("3A218DE7-0C55-4B70-A0D8-823195532B66"),
                    SoLuong = 10,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("BEC142A2-48AB-4785-8295-63A08D7DBF99"),
                    IdSanPhamChiTiet = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSize = new Guid("2D53402B-696D-4E6E-B95E-FB5F2A99C014"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("A18FEBE6-0A68-44CB-8CA0-1514F5BB6D48"),
                    IdSanPhamChiTiet = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSize = new Guid("560CD8D3-52CC-4204-9145-0A142CDAC1FD"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("6B5D160A-2812-4C8C-A63D-0A4535990221"),
                    IdSanPhamChiTiet = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    IdSize = new Guid("9B9A3A86-196E-4FB2-9300-A64784962D59"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },


                //cho nay
                new SizeSanPham()
                {
                    Id = new Guid("A8B2D347-1AF2-4027-8C6C-F368E567675F"),
                    IdSanPhamChiTiet = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSize = new Guid("CD27319D-468D-4189-99A4-B2DA273492A3"),
                    SoLuong = 10,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },

                new SizeSanPham()
                {
                    Id = new Guid("289CE400-38F1-47FC-A5FC-DBF88BF2582E"),
                    IdSanPhamChiTiet = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSize = new Guid("D43ED236-C9CF-4659-BA0D-F834BB316D39"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("A16D0A3A-97E9-4B41-8C6A-54E35CDEACA3"),
                    IdSanPhamChiTiet = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSize = new Guid("D6DC21E2-F1BE-48EC-8F30-BD6BBC73DAEC"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("06DD39CF-F6C7-4605-9D4A-B4F764C29C73"),
                    IdSanPhamChiTiet = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSize = new Guid("9297A9B2-5C69-4B52-AF99-57BBF137DCF1"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("B172BC46-4292-4013-B327-6B2E3DEF9513"),
                    IdSanPhamChiTiet = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSize = new Guid("3A218DE7-0C55-4B70-A0D8-823195532B66"),
                    SoLuong = 10,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("75C5F866-2E78-49BB-80BB-FCD9C7D662C8"),
                    IdSanPhamChiTiet = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSize = new Guid("2D53402B-696D-4E6E-B95E-FB5F2A99C014"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("FBAD9B0B-0E5A-43A0-8FC9-6AEBC586420C"),
                    IdSanPhamChiTiet = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSize = new Guid("560CD8D3-52CC-4204-9145-0A142CDAC1FD"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("FE243159-E3D5-4E38-B4D5-875B908B1D73"),
                    IdSanPhamChiTiet = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    IdSize = new Guid("9B9A3A86-196E-4FB2-9300-A64784962D59"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },


                //cho nay
                new SizeSanPham()
                {
                    Id = new Guid("101C81EE-1CA5-4570-BE45-C453527FA9AA"),
                    IdSanPhamChiTiet = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSize = new Guid("CD27319D-468D-4189-99A4-B2DA273492A3"),
                    SoLuong = 10,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("507812DA-FB24-4429-B50F-C1F5C28D2CC5"),
                    IdSanPhamChiTiet = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSize = new Guid("D43ED236-C9CF-4659-BA0D-F834BB316D39"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("69D226FF-AD05-477F-8AAF-D4E9C8785233"),
                    IdSanPhamChiTiet = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSize = new Guid("D6DC21E2-F1BE-48EC-8F30-BD6BBC73DAEC"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("C5890B6F-463F-4AB3-9DBA-CAB55B32E929"),
                    IdSanPhamChiTiet = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSize = new Guid("9297A9B2-5C69-4B52-AF99-57BBF137DCF1"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("EC43012E-0CA2-4379-BCCF-ABD59824B300"),
                    IdSanPhamChiTiet = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSize = new Guid("3A218DE7-0C55-4B70-A0D8-823195532B66"),
                    SoLuong = 10,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("DAA1D08D-EC42-4DB9-9C91-1EC72A7A7A09"),
                    IdSanPhamChiTiet = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSize = new Guid("2D53402B-696D-4E6E-B95E-FB5F2A99C014"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("1030EA99-2996-4537-B301-3FF511199B66"),
                    IdSanPhamChiTiet = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSize = new Guid("560CD8D3-52CC-4204-9145-0A142CDAC1FD"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("DF277B8D-C5D0-4F62-9DF1-9B60A896F634"),
                    IdSanPhamChiTiet = new Guid("EC94662B-535D-4599-B462-9951C727BE51"),
                    IdSize = new Guid("9B9A3A86-196E-4FB2-9300-A64784962D59"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },


                //cho nay
                new SizeSanPham()
                {
                    Id = new Guid("4D38CD83-32CA-415B-8FBE-88B9A23489C5"),
                    IdSanPhamChiTiet = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSize = new Guid("CD27319D-468D-4189-99A4-B2DA273492A3"),
                    SoLuong = 10,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("EFC5FFAC-F3C6-4C13-A205-51CB97E7A99A"),
                    IdSanPhamChiTiet = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSize = new Guid("D43ED236-C9CF-4659-BA0D-F834BB316D39"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("F10629FF-6D82-462D-9F28-A22AC96FF880"),
                    IdSanPhamChiTiet = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSize = new Guid("D6DC21E2-F1BE-48EC-8F30-BD6BBC73DAEC"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("801A3395-42B9-4874-8144-7D98BD15122E"),
                    IdSanPhamChiTiet = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSize = new Guid("9297A9B2-5C69-4B52-AF99-57BBF137DCF1"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("57987315-CC58-4F70-918F-23858DCEA978"),
                    IdSanPhamChiTiet = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSize = new Guid("3A218DE7-0C55-4B70-A0D8-823195532B66"),
                    SoLuong = 10,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("E0E5863B-7AE4-47D5-9A96-4C0F732AF3F0"),
                    IdSanPhamChiTiet = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSize = new Guid("2D53402B-696D-4E6E-B95E-FB5F2A99C014"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("E1CFF87B-ABD8-4BF0-A7CE-199BD2716FC0"),
                    IdSanPhamChiTiet = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSize = new Guid("560CD8D3-52CC-4204-9145-0A142CDAC1FD"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SizeSanPham()
                {
                    Id = new Guid("69E6C908-EA91-4C4A-86F2-C5CB26FF9EE4"),
                    IdSanPhamChiTiet = new Guid("BCB53BDB-2F7E-4026-A5A3-09FF834562AD"),
                    IdSize = new Guid("9B9A3A86-196E-4FB2-9300-A64784962D59"),
                    SoLuong = 5,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                }
            );
            modelBuilder.Entity<HinhAnh>().HasData(
                new HinhAnh()
                {
                    Id = new Guid("E508BA75-5185-4EF4-B533-2099FFF74A84"),
                    IdSPCT = new Guid("7AAF5675-683D-4608-9534-EA737A4247B3"),
                    LinkAnh = "C:\\Users\\Admin\\OneDrive\\Documents\\Desktop\\Project\\DATN_UD08\\4.CusView\\wwwroot\\img\\PRODUCT\\5.png",
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new HinhAnh()
                {
                    Id = new Guid("B596D1DB-4113-4ED7-99E2-FA96A945D23C"),
                    IdSPCT = new Guid("51A13AFD-08D1-4A20-A0E0-FB4447BD215D"),
                    LinkAnh = "C:\\Users\\Admin\\OneDrive\\Documents\\Desktop\\Project\\DATN_UD08\\4.CusView\\wwwroot\\img\\PRODUCT\\6.jpg",
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                });
            modelBuilder.Entity<SanPham>().HasData(
                new SanPham()
                {
                    Id = new Guid("01BF8A6C-D89A-4DB2-9068-AF277BE488AE"),
                    Ten = "Giay 1",
                    IdHang = new Guid("B9DB8C0C-BF87-4E16-BDD2-A9FEE4B14B4A"),
                    TrangThai = 1,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new SanPham()
                {
                    Id = new Guid("9A1B3786-D0D0-402A-BFD8-10DB04F8E6ED"),
                    Ten = "Giay 2",
                    IdHang = new Guid("DA7A593F-20E5-407E-85C6-4BB0A97F1A73"),
                    TrangThai = 1,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                });
            modelBuilder.Entity<Hang>().HasData(
                new Hang()
                {
                    Id = new Guid("B9DB8C0C-BF87-4E16-BDD2-A9FEE4B14B4A"),
                    TenHang = "Adidas",
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new Hang()
                {
                    Id = new Guid("DA7A593F-20E5-407E-85C6-4BB0A97F1A73"),
                    TenHang = "Nike",
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                }
                );
            modelBuilder.Entity<MauSac>().HasData(
                new MauSac()
                {
                    Id = new Guid("7A6C0C50-FB67-44EA-9C62-AD0E0F67AB3C"),
                    TenMau = "Xanh",
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new MauSac()
                {
                    Id = new Guid("5E5F7D51-044F-4928-9AAC-4A496C629231"),
                    TenMau = "Đỏ",
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new MauSac()
                {
                    Id = new Guid("CFA013B0-34DC-44D5-A471-7C49A9AC167D"),
                    TenMau = "Đen",
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new MauSac()
                {
                    Id = new Guid("8D364A55-877F-45A5-9408-66AFB8AB873F"),
                    TenMau = "Trắng",
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                }
                );
            modelBuilder.Entity<GioHang>().HasData(
                new GioHang()
                {
                    Id = new Guid("9779E43B-FCFC-40A0-B778-FA6F4757FD55"),
                    IdKH = new Guid("3F8C0EFF-A06A-4B07-A449-8832C622290D"),
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new GioHang()
                {
                    Id = new Guid("87C18DE0-9653-408E-9F7D-36B2F5A3D890"),
                    IdKH = new Guid("F572907F-4E58-4EE3-B282-8100A3A2043F"),
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                });
            modelBuilder.Entity<KhachHang>().HasData(
                new KhachHang()
                {
                    Id = new Guid("3F8C0EFF-A06A-4B07-A449-8832C622290D"),
                    Ten = "a",
                    Email = "khachhang@gmail.com",
                    MatKhau = "123456",
                    DiaChi = "1",
                    GioiTinh = true,
                    Sdt = "1",
                    NgaySinh = DateTime.Now,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                new KhachHang()
                {
                    Id = new Guid("F572907F-4E58-4EE3-B282-8100A3A2043F"),
                    Ten = "b",
                    Email = "khachhang01@gmail.com",
                    MatKhau = "123456",
                    DiaChi = "2",
                    GioiTinh = true,
                    Sdt = "2",
                    NgaySinh = DateTime.Now,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                });
            modelBuilder.Entity<MaGiamGia>().HasData(
                new MaGiamGia()
                {
                    Id = new Guid("C76E6A71-F991-4CAB-BA0A-9D899F56A4C0"),
                    Ma = "30",
                    NgayBatdau = DateTime.Now,
                    NgayKetthuc = DateTime.Today.AddDays(30),
                    SoLuong = 5,
                    TrangThai = 0,
                    PhanTramGiam = 30,
                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                    CreateDate = DateTime.Now,
                    isDelete = false,
                },
                                new MaGiamGia()
                                {
                                    Id = new Guid("26A26949-1DFC-42CB-A54F-DBDB44D81685"),
                                    Ma = "20",
                                    NgayBatdau = DateTime.Now,
                                    NgayKetthuc = DateTime.Today.AddDays(30),
                                    SoLuong = 7,
                                    TrangThai = 0,
                                    PhanTramGiam = 20,
                                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                                    CreateDate = DateTime.Now,
                                    isDelete = false,
                                },
                                                                new MaGiamGia()
                                                                {
                                                                    Id = new Guid("71A005C0-0F56-4068-83E3-9C4CC3196B9B"),
                                                                    Ma = "10",
                                                                    NgayBatdau = DateTime.Now,
                                                                    NgayKetthuc = DateTime.Today.AddDays(30),
                                                                    SoLuong = 13,
                                                                    TrangThai = 0,
                                                                    PhanTramGiam = 10,
                                                                    CreateByUserId = new Guid("8BBBF62B-F477-4009-B5D5-1EC7F550F00A"),
                                                                    CreateDate = DateTime.Now,
                                                                    isDelete = false,
                                                                });
        }
    }
}