# docker build -t aks-nodeshelllog .
# docker run -p 8501:8501 -v ~/.kube:/root/.kube aks-nodeshelllog 
import streamlit as st
import subprocess

def run_command(cmd):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    if process.returncode != 0:
        raise Exception(f"Command '{cmd}' failed with error: {error.decode('utf-8')}")
    return output.decode('utf-8').strip()

def get_kube_contexts():
    return run_command("kubectl config get-contexts -o name").split()

def get_nodes_for_context(context):
    return run_command(f"kubectl --context={context} get nodes -o name").split()

def get_logs_for_node(context, node, lines):
    # 여기에서 --context 옵션을 추가하여 해당 컨텍스트의 노드에서 로그를 조회합니다.
    return run_command(f"kubectl node-shell {node} --context {context} -- tail -n {lines} /var/log/messages")


# Streamlit 앱의 메인 함수
def main():
    # Streamlit App Configuration
    st.set_page_config(layout="wide")
    st.title('Kubernetes Node Log Viewer')

    contexts = get_kube_contexts()
    selected_context = st.sidebar.selectbox('Select Kubernetes Context', contexts, key='context_selectbox')

    # "Select Node"와 "Number of log lines"를 같은 행에 배치
    col1, col2 = st.columns(2)

    with col1:
        nodes = get_nodes_for_context(selected_context)
        selected_node = st.selectbox('Select Node', nodes, key='node_selectbox')

    with col2:
        # 사용자가 로그 라인 수를 조정할 수 있도록 숫자 입력 추가
        log_lines = st.number_input('Number of log lines', min_value=1, value=50, step=1)  # 기본값 50, 최소 1부터 시작

    # 로그를 가져오는 함수 호출
    with st.spinner('Logs loading...'):
        logs = get_logs_for_node(selected_context, selected_node, log_lines)

    # 로그 로딩이 완료되면 텍스트 영역에 로그를 표시
    st.text_area("Logs", logs, height=300)

if __name__ == "__main__":
    main()