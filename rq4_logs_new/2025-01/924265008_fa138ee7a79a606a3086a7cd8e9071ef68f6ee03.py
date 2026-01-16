from langchain_core.messages import HumanMessage, AIMessage
from langchain_openai import ChatOpenAI
from typing import TypedDict, List, Dict, Literal
from langgraph.graph import StateGraph, END

# State of the graph
class MedicalState(TypedDict):
    patient_detail: str
    doctor_reply: str
    messages: List[Dict[str, str]]
    current_speaker: Literal["patient", "doctor", "resolution_check"]
    resolved: bool

class MedicalAgents:
    def __init__(self):
        self.patient_model = ChatOpenAI(model="gpt-4o")
        self.doctor_model = ChatOpenAI(model="gpt-4o")

    def patient_node(self, state: MedicalState):
        if len(state["messages"]) == 0:  # Initial question
            new_message = {
                "role": "patient",
                "content": state["patient_detail"]
            }
            return {
                "messages": [new_message],
                "current_speaker": "patient"
            }
        
        # Generate follow-up questions
        prompt = f"""
        As a patient with this initial concern: {state['patient_detail']}
        Continue the conversation based on the doctor's last response: 
        {state['messages'][-1]['content']}
        """
        response = self.patient_model.invoke(prompt)
        new_message = {
            "role": "patient",
            "content": response.content
        }
        new_messages = state["messages"] + [new_message]
        return {
            "messages": new_messages,
            "current_speaker": "patient"
        }

    def doctor_node(self, state: MedicalState):
        # Generate medical response with original tone guidance
        prompt = f"""
        You are a doctor. Your answer's response style should be similar to this: {state['doctor_reply']}
        Patient's history: {state['patient_detail']}
        Conversation history: {[m['content'] for m in state['messages']]}
        """
        response = self.doctor_model.invoke(prompt)
        new_message = {
            "role": "doctor",
            "content": response.content
        }
        new_messages = state["messages"] + [new_message]
        return {
            "messages": new_messages,
            "current_speaker": "doctor"
        }

def should_continue(state: MedicalState) -> str:
    """Determine the next state in the conversation flow."""
    if state.get("resolved", False):
        return END
    # After resolution check, always go to patient if not resolved
    if state["current_speaker"] == "resolution_check":
        return "patient"
    # Normal back-and-forth between patient and doctor
    return "resolution_check" if state["current_speaker"] == "doctor" else "doctor"

def resolve_condition(state: MedicalState) -> MedicalState:
    """Check if the conversation should be resolved."""
    messages_length = len(state.get("messages", []))
    return {
        "resolved": messages_length >= 6,
        "current_speaker": "resolution_check"
    }

def build_workflow():
    workflow = StateGraph(MedicalState)
    agents = MedicalAgents()
    
    workflow.add_node("patient", agents.patient_node)
    workflow.add_node("doctor", agents.doctor_node)
    workflow.add_node("resolution_check", resolve_condition)
    
    workflow.set_entry_point("patient")
    
    # Add edges for the conversation flow
    workflow.add_edge("patient", "doctor")
    workflow.add_edge("doctor", "resolution_check")
    
    # Add conditional edges from resolution check
    workflow.add_conditional_edges(
        "resolution_check",
        should_continue,
        {
            "patient": "patient",
            "doctor": "doctor",
            END: END
        }
    )
    
    return workflow

def generate_cot(row):
    workflow = build_workflow()
    app = workflow.compile()
    
    initial_state = {
        "patient_detail": row["patient_detail"],
        "doctor_reply": row["doctor_reply"],
        "messages": [],
        "current_speaker": "patient",
        "resolved": False
    }
    
    messages = []
    for event in app.stream(initial_state):
        for key, value in event.items():
            if "messages" in value:
                messages = value["messages"]
            if key == "resolution_check" and value.get("resolved", False):
                break
        else:
            continue
        break
    
    conversation_path = "conversation_log.txt"
    with open(conversation_path, "a", encoding="utf-8") as f:
        f.write("----- Full Conversation -----\n\n")
        for i, msg in enumerate(messages):
            speaker = "Patient" if msg["role"] == "patient" else "Doctor"
            f.write(f"{speaker}: {msg['content']}\n\n")
    
    return {
        "question": row["question"],
        "cot": [msg["content"] for msg in messages]
    }

# Process single row
sample_row = {
    "question": """Q. Kindly suggest a homeopathic medicine to stop hairfall and promote hair growth.""",
    "patient_detail": """Hello doctor,
I am 24 years old, and for the past nine years, I am facing hair fall problem. Nowadays, my 60 % of hair is falling on my top and front of my head. I checked my thyroid and hemoglobin many times, but their reports are good. I use many home remedies and hair oils, but when I stop using it, again, it starts falling. The allopathic doctor says use Minoxidil 5 %, Finasteride, and hair serum. But Finasteride has many side effects. Can you please tell me is any medicine available in homeopathy which stops hair fall and promotes hair growth? If yes, can you please tell me the name?""",
    "doctor_reply": """Hello. I checked the attached photo (attachment removed to protect patient identity) and read your description. It seems you have been suffering from hair loss problem for a longtime. Do you eat healthy food like vegetables and fruits every day? Sometimes lack of nutrition is also the reason for hair loss. Do you know if your father also differed from hair loss issue at this young age? Did you suffer from severe health issues or chronic illnesses? If you do not think above mentioned is the cause for your hair fall, then the only reason I can think is that you have been going through severe stress which has taken a toll on your health. Homeopathy would be a good option in this case as it will cure our problem from the root cause, and there will be no side effects. So to prescribe you a correct homeopathic remedy I need to understand your mental, emotional, and physical state. He ce I need detail case history which can be done either through face to face consultation or online consultation. I would advise you to visit a good homeopath for consultation. For the time being, you can apply arnica hair oil twice a week on your hair. Just mix one spoon of Arnica oil with five spoons of coconut oil and apply all over your scalp. You can gently massage your ear with this mixture. Try this for 15 days and then let me know how you feel. I hope you start feeling better. Let me know if you have any questions.
"""
}

result = generate_cot(sample_row)
print("Generated COT:")
for i, message in enumerate(result["cot"]):
    speaker = "Patient" if i % 2 == 0 else "Doctor"
    print(f"{speaker}: {message}")

print(f"\nConversation stored in 'conversation_log.txt'")