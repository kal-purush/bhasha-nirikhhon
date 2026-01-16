using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(AudioSource))]
public class MenuController : MonoBehaviour
{
    [Tooltip("The Game Object that contains the menu buttons")]
    public Transform buttonsLayout;

    [Tooltip("Sound played when change option")]
    public AudioClip changeOptionSound;


    int numberOfButtons;

    [SerializeField] int currentIndex;
    // Start is called before the first frame update
    MenuButton[] buttons;
    Animator[] buttonAnimators;

    bool pressed;
    bool submited;

    AudioSource audiosource;

    void Start()
    {
        currentIndex = 0;
        buttons = buttonsLayout.GetComponentsInChildren<MenuButton>();
        buttonAnimators = buttonsLayout.GetComponentsInChildren<Animator>();
        audiosource = GetComponent<AudioSource>();
        numberOfButtons = buttons.Length;

        ButtonSelected(0);
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetAxis("Vertical") != 0)
        {
            if (pressed == false)
            {
                audiosource.PlayOneShot(changeOptionSound);

                ButtonDeSelected(currentIndex);

                if (Input.GetAxis("Vertical") > 0)
                {
                    currentIndex--;
                }
                else
                {
                    currentIndex++;
                }

                if (currentIndex < 0)
                {
                    currentIndex = numberOfButtons - 1;
                }

                if (currentIndex == numberOfButtons)
                {
                    currentIndex = 0;
                }
                ButtonSelected(currentIndex);


                pressed = true;
            }
        }
        else
        {
            pressed = false;
        }

        if (Input.GetAxis("Submit") == 1)
        {
            if(submited == false){
                Submit(currentIndex);
                submited = true;
            }
        }
        else
        {
            submited = false;
        }
    }


    private void ButtonSelected(int index)
    {
        buttonAnimators[index].SetBool("selected", true);
    }

    private void ButtonDeSelected(int index)
    {
        buttonAnimators[index].SetBool("selected", false);
    }

    private void Submit(int index)
    {
        if (buttons[index].submitSound != null)
        {
            audiosource.PlayOneShot(buttons[index].submitSound);
        }
        buttonAnimators[index].SetTrigger("submit");
        buttons[index].Action();
    }
}