from django import forms

Tech_li = ['ランダム', 'AR', 'VR', 'AI', 'IoT', '5G', '6G', '7G', 'CG', 
            'スマートスピーカー', '3dプリンタ', 'ブロックチェーン', 
            '自動運転', '言語翻訳']

Category_li = ['ランダム', '農業', 'インフラ', 'エネルギー', '医療', '健康',
            '介護' '自動車', '建築', '金融', '物流', '流通', '防犯', 
            '防災', '教育',  '飲食', '林業', 'テクノロジー', '感覚']


Agri_li = ['ランダム', '水耕栽培', '地中海式農業', '畜産',  '酪農', '園芸農業', 
        '混合農業', 'プランテーション', '遊牧', '焼畑', '耕作', 
        '有機栽培', '自然農法', '無農薬', 'バイオダイナミック農法', 
        '植物工場']

Web_service_li = ['ランダム', '情報検索機能', '情報自動所得', 'EC', 'web会議', 
                'データベース', 'クラウド', 'SNS', 'メディア']

AI_li = ['ランダム', '迷惑メールフィルタ', '音声認識', 'レコメンデーション', '会話bot', 
        '音声合成', '自動運転車', '感情推定', '画像認識', '画像変換']

Religion_li = ['ランダム', 'ユダヤ教', 'キリスト教系', 'キリスト教', 'キリスト教系新宗教', 
            'イスラーム', 'インド宗教', 'ヒンドゥー教', 'インドの自由思想家', 
            'ジャイナ教', 'シク教', 'ゾロアスター教', '仏教', 'インド仏教', 
            '南伝仏教', '北伝仏教', '西アジアの宗教', '中央アジアの宗教', 
            '東アジアの宗教', '精神修養', '土着宗教など', 'ネオペイガニズム', 
            'アフロ・クレオール宗教', 'サタニズム系', '秘教', '神秘主義', 
                'パロディー宗教', '非宗教、反宗教、メタ宗教']

# Feeling_li = ['ランダム', '触覚', '味覚', '嗅覚', '視覚', '聴覚']
Feeling_li = (
    ('random', 'ランダム'), ('touch', '触覚'),('tong', '味覚'),('nose', '嗅覚'),('eye', '視覚'), ('ear', '聴覚')
    )



class CategoryForm(forms.Form):
    # tech = forms.ChoiceField(
    #     label='テクノロジー',
    #     widget=forms.SelectMultiple,
    #     choices=Tech_li,
    #     required=True,
    # )
    # agri = forms.ChoiceField(
    #     label='農業',
    #     widget=forms.SelectMultiple,
    #     choices=Agri_li,
    #     required=True,
    # )
    # web = forms.ChoiceField(
    #     label='Webサービス',
    #     widget=forms.SelectMultiple,
    #     choices=Web_service_li,
    #     required=True,
    # )
    # AI = forms.ChoiceField(
    #     label='AI',
    #     widget=forms.SelectMultiple,
    #     choices=AI_li,
    #     required=True,
    # )
    feeling = forms.ChoiceField(
        label='五感',
        widget=forms.SelectMultiple,
        choices=Feeling_li,
        required=True,
    )
class HelloForm(forms.Form):
    your_name = forms.CharField(
        label='名前',
        max_length=20,
        required=True,
        widget=forms.TextInput()
    )