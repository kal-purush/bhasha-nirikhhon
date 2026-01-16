/*------------------------------------------SKILLs STARTS---------------------------------------------*/

const skillsContent = document.getElementsByClassName('skill_content'),
      skillsHeader  = document.getElementsByClassName('skills_header')

function toggleSkills()
{
    let itemClass = this.parenNode.className

    for(i = 0; i <skillsContent.length; i++)
    {
        skillsContent[i].className = 'skill_content skills_open'
    }
    if(itemClass === 'skill_content skills_close')
    {
        this.parenNode.className = 'skill_content skills_open'
    }
}

skillsHeader.forEach((el) =>
{
    el.addEventListener('click', toggleSkills)
})