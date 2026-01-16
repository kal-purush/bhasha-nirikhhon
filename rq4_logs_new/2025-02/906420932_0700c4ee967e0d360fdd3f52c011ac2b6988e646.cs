using UnityEngine;
using UnityEditor;

public class HeliumShaderUI : ShaderGUI {

    Material target;
    MaterialEditor editor;
	MaterialProperty[] properties;
    static GUIContent staticLabel = new GUIContent();

    bool dirty = false;
    static GUIContent MakeLabel (string text, string tooltip = null) {
		staticLabel.text = text;
		staticLabel.tooltip = tooltip;
		return staticLabel;
	}

    static GUIContent MakeLabel (
		MaterialProperty property, string tooltip = null
	) {
		staticLabel.text = property.displayName;
		staticLabel.tooltip = tooltip;
		return staticLabel;
	}
    public override void OnGUI (
		MaterialEditor editor, MaterialProperty[] properties
	) {
        this.target = editor.target as Material;
		this.editor = editor;
		this.properties = properties;
        DoMain();
	}
	
	void DoMain() {
        GUILayout.Label("Main Maps", EditorStyles.boldLabel);
        MaterialProperty mainTex = FindProperty("_Tex");
        MaterialProperty tint = FindProperty("_Color");
		
        editor.TexturePropertySingleLine(MakeLabel(mainTex, "Albedo (RGB)"), mainTex, tint);
        DoPackedMetallicRoughness();
		DoRoughness();
		DoMetallic();
        if(dirty){
            dirty = false;
        }
        DoNormals();
        DoSecondary();
        DoSecondaryNormals();
        editor.TextureScaleOffsetProperty(mainTex);

	}
    void DoNormals () {
		MaterialProperty map = FindProperty("_Normal");
        editor.TexturePropertySingleLine(
			MakeLabel(map), map, map.textureValue ? FindProperty("_NormalStrength"): null
		);
	}

    void DoPackedMetallicRoughness(){
        MaterialProperty map = FindProperty("_PackedMR");
		EditorGUI.BeginChangeCheck();
        editor.TexturePropertySingleLine(
            MakeLabel(map, "Metallic (R)"), map,
            null
        );
		if (EditorGUI.EndChangeCheck()) {
            SetKeyword("HELIUM_PACKED_MR", map.textureValue);
            dirty = map.textureValue;
		}
    }

    void DoMetallic () {
        MaterialProperty map = FindProperty("_Metallic");
        MaterialProperty packedMap = FindProperty("_PackedMR");
        bool useTex = map.textureValue || packedMap.textureValue;
		EditorGUI.BeginChangeCheck();
        if (!packedMap.textureValue){
            editor.TexturePropertySingleLine(
                MakeLabel(map, "Metallic (R)"), map,
                map.textureValue ? null : FindProperty("_UniformMetallic")
            );
        }
		if (EditorGUI.EndChangeCheck() || dirty ) {
            SetKeyword("HELIUM_2D_METALLIC", useTex);
		}
        
	}

	void DoRoughness () {
        MaterialProperty map = FindProperty("_Roughness");
        MaterialProperty packedMap = FindProperty("_PackedMR");
        bool useTex = map.textureValue || packedMap.textureValue;
		EditorGUI.BeginChangeCheck();
        if (!packedMap.textureValue){
            editor.TexturePropertySingleLine(
                MakeLabel(map, "Roughness (1-G)"), map,
                map.textureValue ? null : FindProperty("_UniformRoughness")
            );
        }
		if (EditorGUI.EndChangeCheck() || dirty) {
            SetKeyword("HELIUM_2D_ROUGHNESS", useTex);
		}
	}
    void DoSecondary () {
		GUILayout.Label("Secondary Maps", EditorStyles.boldLabel);

		MaterialProperty detailTex = FindProperty("_SecondaryTex");
		editor.TexturePropertySingleLine(
			MakeLabel(detailTex, "Albedo (RGB) multiplied by 2"), detailTex
		);
		editor.TextureScaleOffsetProperty(detailTex);
	}

    void DoSecondaryNormals () {
		MaterialProperty map = FindProperty("_SecondaryNormal");
		editor.TexturePropertySingleLine(
			MakeLabel(map), map,
			map.textureValue ? FindProperty("_SecondaryNormalStrength") : null
		);
	}

    MaterialProperty FindProperty (string name) {
		return FindProperty(name, properties);
	}
    void SetKeyword (string keyword, bool state) {
        if (state) {
            target.EnableKeyword(keyword);
        }
        else {
            target.DisableKeyword(keyword);
        }
	}
}