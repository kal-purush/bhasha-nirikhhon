import * as React from "react";
import Box from "@mui/material/Box";
import {List, ListItem, ListItemButton, ListItemIcon, ListItemText} from "@mui/material";
import CheckBoxOutlineBlankIcon  from '@mui/icons-material/CheckBoxOutlineBlank';
import AssignmentIcon from '@mui/icons-material/Assignment';
import DownloadIcon from '@mui/icons-material/Download';

export default function FormsList() {


    return (<Box sx={{ width: '80%', bgcolor: 'background.paper', marginLeft: '20%' }}>
            <nav aria-label="main mailbox folders">
                <List>
                    <ListItem >
                        <ListItemIcon>
                            <AssignmentIcon  />
                        </ListItemIcon>
                        <ListItemText primary="Land Use  Permit Application" />
                        <ListItemIcon>
                    <a href="https://cdn.fbsbx.com/v/t59.2708-21/295519717_3244537395822367_6216380734588819315_n.pdf/home-occupation-class-2.pdf?_nc_cat=108&ccb=1-7&_nc_sid=0cab14&_nc_ohc=afsPiUiwl9YAX8yz-bm&_nc_ht=cdn.fbsbx.com&oh=03_AVLBHwMfk7WTsmGY5g6Ct1NXZSc7LypjlWkwELuGR1hUsw&oe=62DEDAA9&dl=1"><DownloadIcon/></a>
                        </ListItemIcon>
                    </ListItem>
                    <ListItem >
                        <ListItemIcon>
                            <AssignmentIcon/>
                        </ListItemIcon>
                        <ListItemText primary="Street Use Permit Application" />
                        <ListItemIcon>
                                                <a href="https://cdn.fbsbx.com/v/t59.2708-21/295519717_3244537395822367_6216380734588819315_n.pdf/home-occupation-class-2.pdf?_nc_cat=108&ccb=1-7&_nc_sid=0cab14&_nc_ohc=afsPiUiwl9YAX8yz-bm&_nc_ht=cdn.fbsbx.com&oh=03_AVLBHwMfk7WTsmGY5g6Ct1NXZSc7LypjlWkwELuGR1hUsw&oe=62DEDAA9&dl=1"><DownloadIcon/></a>
                        </ListItemIcon>
                    </ListItem>
                    <ListItem >
                        <ListItemIcon>
                            <AssignmentIcon/>
                        </ListItemIcon>
                        <ListItemText primary="Class 2 Development Permit Application" />
                        <ListItemIcon>
                                                <a href="https://cdn.fbsbx.com/v/t59.2708-21/295519717_3244537395822367_6216380734588819315_n.pdf/home-occupation-class-2.pdf?_nc_cat=108&ccb=1-7&_nc_sid=0cab14&_nc_ohc=afsPiUiwl9YAX8yz-bm&_nc_ht=cdn.fbsbx.com&oh=03_AVLBHwMfk7WTsmGY5g6Ct1NXZSc7LypjlWkwELuGR1hUsw&oe=62DEDAA9&dl=1"><DownloadIcon/></a>
                        </ListItemIcon>
                    </ListItem>
                    <ListItem >
                        <ListItemIcon>
                            <AssignmentIcon/>
                        </ListItemIcon>
                        <ListItemText primary="Fire Inspection Request" />
                        <ListItemIcon>
                                                <a href="https://cdn.fbsbx.com/v/t59.2708-21/295519717_3244537395822367_6216380734588819315_n.pdf/home-occupation-class-2.pdf?_nc_cat=108&ccb=1-7&_nc_sid=0cab14&_nc_ohc=afsPiUiwl9YAX8yz-bm&_nc_ht=cdn.fbsbx.com&oh=03_AVLBHwMfk7WTsmGY5g6Ct1NXZSc7LypjlWkwELuGR1hUsw&oe=62DEDAA9&dl=1"><DownloadIcon/></a>
                        </ListItemIcon>
                    </ListItem>
                    <ListItem >
                        <ListItemIcon>
                            <AssignmentIcon/>
                        </ListItemIcon>
                        <ListItemText primary="AHS Inspection Request" />
                        <ListItemIcon>
                                                <a href="https://cdn.fbsbx.com/v/t59.2708-21/295519717_3244537395822367_6216380734588819315_n.pdf/home-occupation-class-2.pdf?_nc_cat=108&ccb=1-7&_nc_sid=0cab14&_nc_ohc=afsPiUiwl9YAX8yz-bm&_nc_ht=cdn.fbsbx.com&oh=03_AVLBHwMfk7WTsmGY5g6Ct1NXZSc7LypjlWkwELuGR1hUsw&oe=62DEDAA9&dl=1"><DownloadIcon/></a>
                        </ListItemIcon>
                    </ListItem>
                    <ListItem >
                        <ListItemIcon>
                            <AssignmentIcon/>
                        </ListItemIcon>
                        <ListItemText primary="AMVIC Licence Application" />
                        <ListItemIcon>
                                                <a href="https://cdn.fbsbx.com/v/t59.2708-21/295519717_3244537395822367_6216380734588819315_n.pdf/home-occupation-class-2.pdf?_nc_cat=108&ccb=1-7&_nc_sid=0cab14&_nc_ohc=afsPiUiwl9YAX8yz-bm&_nc_ht=cdn.fbsbx.com&oh=03_AVLBHwMfk7WTsmGY5g6Ct1NXZSc7LypjlWkwELuGR1hUsw&oe=62DEDAA9&dl=1"><DownloadIcon/></a>
                        </ListItemIcon>
                    </ListItem>
                    <ListItem >
                        <ListItemIcon>
                            <AssignmentIcon/>
                        </ListItemIcon>
                        <ListItemText primary="Planning Approval Application" />
                        <ListItemIcon>
                                                <a href="https://cdn.fbsbx.com/v/t59.2708-21/295519717_3244537395822367_6216380734588819315_n.pdf/home-occupation-class-2.pdf?_nc_cat=108&ccb=1-7&_nc_sid=0cab14&_nc_ohc=afsPiUiwl9YAX8yz-bm&_nc_ht=cdn.fbsbx.com&oh=03_AVLBHwMfk7WTsmGY5g6Ct1NXZSc7LypjlWkwELuGR1hUsw&oe=62DEDAA9&dl=1"><DownloadIcon/></a>
                        </ListItemIcon>
                    </ListItem>
                </List>
            </nav>
        </Box>

    );
}