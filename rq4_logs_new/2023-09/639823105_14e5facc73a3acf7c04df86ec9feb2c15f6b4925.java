package com.teachsync.controllers;

import com.teachsync.dtos.address.AddressCreateDTO;
import com.teachsync.dtos.address.AddressReadDTO;
import com.teachsync.dtos.address.AddressUpdateDTO;
import com.teachsync.dtos.center.CenterReadDTO;
import com.teachsync.dtos.staff.StaffReadDTO;
import com.teachsync.dtos.staff.StaffUpdateDTO;
import com.teachsync.dtos.user.UserReadDTO;
import com.teachsync.dtos.user.UserUpdateAccountDTO;
import com.teachsync.dtos.user.UserUpdateDTO;
import com.teachsync.entities.LocationUnit;
import com.teachsync.services.address.AddressService;
import com.teachsync.services.center.CenterService;
import com.teachsync.services.locationUnit.LocationUnitService;
import com.teachsync.services.staff.StaffService;
import com.teachsync.services.user.UserService;
import com.teachsync.utils.enums.DtoOption;
import com.teachsync.utils.enums.Gender;
import jakarta.servlet.http.HttpSession;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Controller
public class StaffController {
    @Autowired
    CenterService centerService;

    @Autowired
    StaffService staffService;

    @Autowired
    UserService userService;

    @Autowired
    private AddressService addressService;

    @Autowired
    private LocationUnitService locationUnitService;

    @Autowired
    private ModelMapper mapper;

    @GetMapping("/list-staff")
    public String staffListPage(
            Model model,
            @RequestParam Long id
    ){
        try{
            CenterReadDTO centerReadDTO = centerService.getDTOById(id,null);
            List<StaffReadDTO> staffList = staffService.getAllDTOByCenterId(centerReadDTO.getId(),null);
            for(int i =0; i<staffList.size();i++){
                UserReadDTO user = userService.getDTOById(staffList.get(i).getUserId(),null);
                staffList.get(i).setUser(user);
            }

            model.addAttribute("center", centerReadDTO);
            model.addAttribute("staffList",staffList);
        }catch (Exception e){
            e.printStackTrace();
        }

        return "staff/list-staff";
    }

    @GetMapping("/staff-detail")
    public String staffDetailPage(
            Model model,
            Long id,
            RedirectAttributes redirect,
            @SessionAttribute(value = "user", required = false) UserReadDTO userDTO
    ) {

        /* Check login */
        if (ObjectUtils.isEmpty( userDTO)) {
            redirect.addAttribute("mess", "Làm ơn đăng nhập");
            return "redirect:/index";
        }

        try {
            StaffReadDTO staffReadDTO = staffService.getDTOById(id, List.of(DtoOption.USER));
            List<LocationUnit> countryList = locationUnitService.getAllByLevel(0);
            model.addAttribute("countryList", countryList);

            Long addressId = staffReadDTO.getUser().getAddressId();
            if (addressId != null) {
                AddressReadDTO addressDTO = addressService.getDTOById(addressId, null);
                model.addAttribute("address", addressDTO);

                Long wardId = addressDTO.getUnitId();
                Map<Integer, Long> levelUnitIdMap =
                        locationUnitService.mapLevelUnitIdByBottomChildId(wardId, null);
                model.addAttribute("levelUnitIdMap", levelUnitIdMap);

                List<LocationUnit> provinceList = locationUnitService.getAllByParentId(levelUnitIdMap.get(0));
                model.addAttribute("provinceList", provinceList);

                List<LocationUnit> districtList = locationUnitService.getAllByParentId(levelUnitIdMap.get(1));;
                model.addAttribute("districtList", districtList);

                List<LocationUnit> wardList = locationUnitService.getAllByParentId(levelUnitIdMap.get(2));
                model.addAttribute("wardList", wardList);
            } else {
                Long parentId = countryList.get(0).getId();
                List<LocationUnit> provinceList = locationUnitService.getAllByParentId(parentId);
                model.addAttribute("provinceList", provinceList);

                parentId = provinceList.get(0).getId();
                List<LocationUnit> districtList = locationUnitService.getAllByParentId(parentId);;
                model.addAttribute("districtList", districtList);

                parentId = districtList.get(0).getId();
                List<LocationUnit> wardList = locationUnitService.getAllByParentId(parentId);
                model.addAttribute("wardList", wardList);
            }
            model.addAttribute("staff",staffReadDTO);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "staff/staff-detail";
    }

    @PostMapping("/staff-detail/{option}")
    public String editProfile(
            @PathVariable("option") String option,
            RedirectAttributes redirect,
            @RequestParam(value = "staffId", required = false) Long staffId,
            @RequestParam(value = "userAvatar", required = false) String userAvatar,
            @RequestParam(value = "about", required = false) String about,
            @RequestParam(value = "fullName", required = false) String fullName,
            @RequestParam(value = "email", required = false) String email,
            @RequestParam(value = "phone", required = false) String phone,
            @RequestParam(value = "gender", required = false) Gender gender,
            @RequestParam(value = "addressNo", required = false) String addressNo,
            @RequestParam(value = "street", required = false) String street,
            @RequestParam(value = "unitId", required = false) Long unitId,
            HttpSession session,
            @SessionAttribute(value = "user", required = false) UserReadDTO userDTO) {

        /* Check login */
        if (ObjectUtils.isEmpty(userDTO)) {
            redirect.addAttribute("mess", "Làm ơn đăng nhập");
            return "redirect:/index";
        }

        try {
            StaffReadDTO staffReadDTO = staffService.getDTOById(staffId,List.of(DtoOption.USER));
            StaffUpdateDTO staffUpdateDTO = mapper.map(staffReadDTO,StaffUpdateDTO.class);
            UserUpdateDTO userUpdateDTO = mapper.map(staffReadDTO.getUser(), UserUpdateDTO.class);
            staffUpdateDTO.setUpdatedBy(userDTO.getId());
            userUpdateDTO.setUpdatedBy(userDTO.getId());

            switch (option) {
                case "avatar" -> {
                    userUpdateDTO.setUserAvatar(userAvatar);
                    userUpdateDTO.setAbout(about);
                }

                case "detail" -> {
                    userUpdateDTO.setFullName(fullName);
                    userUpdateDTO.setEmail(email);
                    userUpdateDTO.setPhone(phone);
                    userUpdateDTO.setGender(gender);
                }

                case "address" -> {
                    AddressReadDTO addressReadDTO;

                    if (staffReadDTO.getUser().getAddressId() == null) {
                        AddressCreateDTO addressCreateDTO = new AddressCreateDTO();
                        addressCreateDTO.setAddressNo(addressNo);
                        addressCreateDTO.setStreet(street);
                        addressCreateDTO.setUnitId(unitId);
                        addressCreateDTO.setCreatedBy(staffReadDTO.getUser().getId());

                        addressReadDTO = addressService.createAddressByDTO(addressCreateDTO);

                        userUpdateDTO.setAddressId(addressReadDTO.getId());
                    } else {
                        addressReadDTO = addressService.getDTOById(staffReadDTO.getUser().getAddressId(), null);

                        AddressUpdateDTO addressUpdateDTO = mapper.map(addressReadDTO, AddressUpdateDTO.class);

                        addressService.updateAddressByDTO(addressUpdateDTO);
                    }
                }
            }

            staffReadDTO.setUser(userService.updateDTOUser(userUpdateDTO));
            staffUpdateDTO.setUser(staffReadDTO.getUser());
            staffService.updateStaffByDTO(staffUpdateDTO);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "redirect:/staff-detail?id="+staffId;
    }

    @PutMapping("/api/staff-detail/{option}")
    @ResponseBody
    public Map<String, String> editAccount(
            @PathVariable("option") String option,
            HttpSession session,
            @RequestBody UserUpdateAccountDTO accountDTO,
            @SessionAttribute(value = "user", required = false) UserReadDTO userDTO) {

        Map<String, String> response = new HashMap<>();

        try {
            UserUpdateDTO userUpdateDTO = mapper.map(userDTO, UserUpdateDTO.class);
            userUpdateDTO.setUpdatedBy(userDTO.getId());

            switch (option) {
                case "username" -> {
                    if (userService.getAllByUsernameAndIdNot(accountDTO.getUsername(), userDTO.getId()) != null) {
                        throw new IllegalArgumentException("Tên tài khoản này đã được sử dụng. Xin chọn tên khác.");
                    }

                    userService.loginDTO(userDTO.getUsername(), accountDTO.getPassword());

                    userUpdateDTO.setUsername(accountDTO.getUsername());
                }

                case "password" -> {
                    if (accountDTO.getPassword().equals(accountDTO.getOldPassword())) {
                        throw new IllegalArgumentException("Mật khẩu mới giống mật khẩu cũ. Hủy thay đổi.");
                    }
                    userService.loginDTO(userDTO.getUsername(), accountDTO.getOldPassword());

                    userUpdateDTO.setPassword(accountDTO.getPassword());
                }
            }

            userDTO = userService.updateDTOUser(userUpdateDTO);
            session.setAttribute("user", userDTO);
            response.put("msg", "Cập nhập thành công.");
        } catch (BadCredentialsException bCE) {
            bCE.printStackTrace();
            response.put("error", "Sai mật khẩu.");
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            response.put("error", e.getMessage());
            return response;
        }

        response.put("view", "/profile");
        return response;
    }


}