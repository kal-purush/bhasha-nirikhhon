import { Box, Switch } from '@mui/material';
import { Icon } from '@iconify/react';
import useTheme from '../context/theme';
export default function BasicCard(){

    const {themeMode, applyDarkTheme, applyLightTheme} = useTheme()
    
    
    
    return(
        <Box sx={{display:'flex', justifyContent:'end', alignItems :'center', width : '100vw', padding : '20px' }}>
            <Icon icon="ph:sun-duotone" />
            <Switch checked={themeMode === 'dark'} color="primary" onChange={(e)=>{
                if(e.currentTarget.checked)
                {
                
                    applyDarkTheme()

                }
                else{
                    applyLightTheme()
                    
                }
                localStorage.setItem('darkTheme',e.currentTarget.checked )
            

            }}/>
            <Icon icon="solar:moon-bold" />

        </Box>
    )
}