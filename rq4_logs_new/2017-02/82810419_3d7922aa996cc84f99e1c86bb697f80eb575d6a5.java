/*
 * Copyright 2002-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bop.fractals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.bop.fractals.line.FractalLine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Marco Ruiz
 * @since Feb 26, 2017
 */
public class GeometricPatternFractalGeneratorTest {

	private List<FractalLine> pattern = getPattern();
	private List<FractalLine> expected;
	private List<FractalLine> computed = getComputedFractal();

	@Before
	public void setUp() throws Exception {
		expected = getExpectedFractal();
	}

	@Test
	public final void testGetFractal() {
		Assert.assertEquals(expected.size() + pattern.size() - 1, computed.size());
		for (FractalLine line : computed)
			expected.remove(line);

		Assert.assertEquals(0, expected.size());
	}

	private final List<FractalLine> getComputedFractal() {
		GeometricPatternFractalGenerator<FractalLine> generator =
				new GeometricPatternFractalGenerator<FractalLine>(getPattern(), 5, false, null);
		generator.generateFractalSync();
		return generator.getFractal();
	}

	private final List<FractalLine> getPattern() {
		List<FractalLine> result = new ArrayList<FractalLine>();

		result.add(new FractalLine(337.0f, 536.0f, 312.0f, 257.0f));
		result.add(new FractalLine(310.0f, 364.0f, 338.0f, 191.0f));
		result.add(new FractalLine(321.0f, 497.0f, 151.0f, 456.0f));
		result.add(new FractalLine(345.0f, 496.0f, 492.0f, 395.0f));

		return result;
	}

	private final List<FractalLine> getExpectedFractal() {
		List<FractalLine> result = new ArrayList<FractalLine>();

		result.add(new FractalLine(321.0368f, 340.98898f, 425.77246f, 302.49933f));
		result.add(new FractalLine(306.33307f, 337.8829f, 209.61893f, 286.73288f));
		result.add(new FractalLine(294.73618f, 498.66455f, 236.49774f, 591.0052f));
		result.add(new FractalLine(361.64313f, 474.94293f, 331.30356f, 367.81213f));
		result.add(new FractalLine(320.23697f, 255.55528f, 363.97028f, 155.01167f));
		result.add(new FractalLine(213.88019f, 478.57706f, 117.16603f, 427.42706f));
		result.add(new FractalLine(432.0049f, 427.3051f, 536.74054f, 388.81546f));
		result.add(new FractalLine(298.94348f, 484.27094f, 268.60394f, 377.14014f));
		result.add(new FractalLine(369.69287f, 487.9474f, 471.64056f, 537.9714f));
		result.add(new FractalLine(322.0538f, 239.1556f, 271.33636f, 193.18826f));
		result.add(new FractalLine(330.48694f, 243.31288f, 399.9324f, 236.18037f));
		result.add(new FractalLine(197.70128f, 475.52368f, 148.11337f, 522.4934f));
		result.add(new FractalLine(202.47818f, 467.44888f, 200.65681f, 397.8121f));
		result.add(new FractalLine(448.2196f, 426.24298f, 502.28384f, 472.33722f));
		result.add(new FractalLine(343.2168f, 191.39827f, 385.28036f, 137.21217f));
		result.add(new FractalLine(151.79254f, 450.83975f, 101.075096f, 404.87244f));
		result.add(new FractalLine(495.37393f, 399.11926f, 564.81946f, 391.98672f));
		result.add(new FractalLine(244.24542f, 310.14557f, 193.528f, 264.17822f));
		result.add(new FractalLine(262.42255f, 558.3319f, 212.83464f, 605.30164f));
		result.add(new FractalLine(338.4131f, 409.7527f, 336.5917f, 340.11584f));
		result.add(new FractalLine(384.40582f, 312.80313f, 453.85132f, 305.6706f));
		result.add(new FractalLine(294.93106f, 326.7547f, 293.1097f, 257.11792f));
		result.add(new FractalLine(282.79803f, 509.1637f, 213.37433f, 505.68616f));
		result.add(new FractalLine(361.81802f, 458.7294f, 411.78644f, 408.4394f));
		result.add(new FractalLine(337.2515f, 339.92688f, 391.31573f, 386.0211f));
		result.add(new FractalLine(299.1184f, 468.0574f, 349.0868f, 417.7674f));
		result.add(new FractalLine(445.35123f, 417.1144f, 443.52988f, 347.4776f));
		result.add(new FractalLine(381.87225f, 499.13388f, 385.9528f, 571.32294f));
		result.add(new FractalLine(290.15417f, 334.82953f, 240.56622f, 381.79922f));
		result.add(new FractalLine(275.71347f, 419.0807f, 273.89212f, 349.44385f));
		result.add(new FractalLine(290.4692f, 514.5299f, 333.43515f, 567.4354f));
		result.add(new FractalLine(434.97986f, 514.77075f, 489.0441f, 560.865f));
		result.add(new FractalLine(352.51764f, 460.88986f, 283.0939f, 457.41235f));
		result.add(new FractalLine(334.38312f, 330.79828f, 332.56177f, 261.16147f));
		result.add(new FractalLine(289.818f, 470.21786f, 220.39426f, 466.74033f));
		result.add(new FractalLine(386.57416f, 490.6062f, 436.54254f, 440.31616f));
		result.add(new FractalLine(346.85486f, 181.73761f, 323.2195f, 146.02678f));
		result.add(new FractalLine(142.45699f, 446.48624f, 105.13102f, 467.28967f));
		result.add(new FractalLine(505.04092f, 395.00592f, 514.7081f, 352.50952f));
		result.add(new FractalLine(234.90987f, 305.79202f, 197.5839f, 326.59543f));
		result.add(new FractalLine(257.38184f, 567.2897f, 275.24493f, 606.0075f));
		result.add(new FractalLine(335.0548f, 399.82205f, 293.50723f, 386.97556f));
		result.add(new FractalLine(367.07117f, 156.06004f, 400.952f, 129.71812f));
		result.add(new FractalLine(118.44465f, 424.42145f, 94.80931f, 388.71063f));
		result.add(new FractalLine(538.14874f, 391.83447f, 581.3507f, 398.25238f));
		result.add(new FractalLine(210.89755f, 283.72726f, 187.2622f, 248.01642f));
		result.add(new FractalLine(233.60442f, 589.5047f, 196.27846f, 610.30817f));
		result.add(new FractalLine(334.4143f, 366.64038f, 344.08145f, 324.14398f));
		result.add(new FractalLine(394.0728f, 308.6898f, 403.74f, 266.19342f));
		result.add(new FractalLine(351.32407f, 185.56224f, 394.52597f, 191.98015f));
		result.add(new FractalLine(272.35522f, 409.15005f, 230.80762f, 396.30356f));
		result.add(new FractalLine(427.1806f, 305.51834f, 470.3825f, 311.93625f));
		result.add(new FractalLine(444.80222f, 518.9937f, 482.87244f, 496.23636f));
		result.add(new FractalLine(271.7147f, 375.96838f, 281.3819f, 333.47202f));
		result.add(new FractalLine(312.71826f, 234.80206f, 275.39227f, 255.60548f));
		result.add(new FractalLine(192.66058f, 484.4814f, 210.52367f, 523.1993f));
		result.add(new FractalLine(441.99298f, 407.18378f, 400.44537f, 394.33728f));
		result.add(new FractalLine(285.11346f, 343.78726f, 302.97653f, 382.50513f));
		result.add(new FractalLine(298.99884f, 520.22565f, 338.87817f, 505.3937f));
		result.add(new FractalLine(342.38202f, 463.47684f, 326.44437f, 503.8389f));
		result.add(new FractalLine(331.02487f, 320.86765f, 289.47726f, 308.02115f));
		result.add(new FractalLine(279.68237f, 472.80484f, 263.74472f, 513.1669f));
		result.add(new FractalLine(146.6018f, 442.33008f, 156.26898f, 399.8337f));
		result.add(new FractalLine(391.52194f, 481.15417f, 371.77127f, 441.54742f));
		result.add(new FractalLine(470.4086f, 541.1298f, 496.05316f, 577.4353f));
		result.add(new FractalLine(340.15393f, 239.19954f, 349.8211f, 196.70319f));
		result.add(new FractalLine(288.7059f, 212.73729f, 265.0706f, 177.02647f));
		result.add(new FractalLine(505.36777f, 400.98346f, 531.0124f, 437.2889f));
		result.add(new FractalLine(168.88315f, 506.6965f, 131.55719f, 527.49994f));
		result.add(new FractalLine(239.05469f, 301.63586f, 248.72188f, 259.13953f));
		result.add(new FractalLine(441.35245f, 374.00214f, 451.01965f, 331.5058f));
		result.add(new FractalLine(199.11992f, 457.51825f, 157.57236f, 444.67175f));
		result.add(new FractalLine(261.33603f, 366.00232f, 224.01003f, 386.8057f));
		result.add(new FractalLine(458.04196f, 430.46594f, 496.11218f, 407.7086f));
		result.add(new FractalLine(319.29565f, 545.5705f, 337.15875f, 584.2884f));
		result.add(new FractalLine(291.5728f, 316.82407f, 250.02524f, 303.97757f));
		result.add(new FractalLine(253.56151f, 562.85004f, 212.01396f, 550.0036f));
		result.add(new FractalLine(272.6624f, 511.75067f, 256.72476f, 552.11273f));
		result.add(new FractalLine(341.027f, 399.95093f, 379.09723f, 377.19354f));
		result.add(new FractalLine(394.39966f, 314.66733f, 420.04422f, 350.97275f));
		result.add(new FractalLine(278.3274f, 409.27893f, 316.39764f, 386.52155f));
		result.add(new FractalLine(440.63284f, 523.436f, 431.9404f, 567.82904f));
		result.add(new FractalLine(316.86304f, 230.64592f, 326.53024f, 188.14955f));
		result.add(new FractalLine(188.84024f, 480.0418f, 147.29271f, 467.1953f));
		result.add(new FractalLine(309.31918f, 461.59363f, 267.7716f, 448.74716f));
		result.add(new FractalLine(447.96515f, 407.31265f, 486.03537f, 384.55527f));
		result.add(new FractalLine(330.38434f, 287.686f, 340.05154f, 245.18964f));
		result.add(new FractalLine(281.29315f, 339.34766f, 239.7456f, 326.50116f));
		result.add(new FractalLine(246.61954f, 470.9216f, 205.07196f, 458.0751f));
		result.add(new FractalLine(415.49146f, 457.35852f, 453.56165f, 434.60114f));
		result.add(new FractalLine(373.26172f, 236.02812f, 416.4636f, 242.44603f));
		result.add(new FractalLine(198.47939f, 424.33664f, 208.14658f, 381.84027f));
		result.add(new FractalLine(483.64835f, 452.60208f, 509.2929f, 488.90753f));
		result.add(new FractalLine(290.93228f, 283.64243f, 300.5995f, 241.1461f));
		result.add(new FractalLine(239.5996f, 509.86743f, 198.05203f, 497.02094f));
		result.add(new FractalLine(390.73532f, 425.48172f, 428.80554f, 402.7244f));
		result.add(new FractalLine(372.68024f, 366.28595f, 398.3248f, 402.5914f));
		result.add(new FractalLine(328.0357f, 434.80972f, 366.1059f, 412.0524f));
		result.add(new FractalLine(387.3773f, 543.7359f, 378.68488f, 588.1289f));
		result.add(new FractalLine(366.7658f, 449.27737f, 347.01517f, 409.67062f));
		result.add(new FractalLine(347.07385f, 344.14984f, 385.14404f, 321.39246f));
		result.add(new FractalLine(304.0662f, 458.60538f, 284.31555f, 418.99866f));
		result.add(new FractalLine(385.66882f, 509.33102f, 429.1775f, 521.35175f));
		result.add(new FractalLine(294.29144f, 523.68945f, 278.35382f, 564.05145f));
		result.add(new FractalLine(342.9639f, 457.5446f, 323.21323f, 417.93784f));
		result.add(new FractalLine(336.99704f, 320.99652f, 375.06726f, 298.23914f));
		result.add(new FractalLine(280.26425f, 466.87256f, 260.5136f, 427.26584f));
		result.add(new FractalLine(395.6251f, 485.63998f, 439.1338f, 497.66068f));
		result.add(new FractalLine(340.48074f, 245.17706f, 366.12534f, 281.4825f));
		result.add(new FractalLine(205.0921f, 457.64713f, 243.1623f, 434.88977f));
		result.add(new FractalLine(453.8726f, 434.90823f, 445.18015f, 479.30124f));
		result.add(new FractalLine(297.54498f, 316.95294f, 335.61517f, 294.1956f));
		result.add(new FractalLine(273.2443f, 505.8184f, 253.49365f, 466.21167f));
		result.add(new FractalLine(370.86896f, 453.76318f, 414.37766f, 465.78387f));
		result.add(new FractalLine(342.90448f, 348.59213f, 334.21207f, 392.98514f));
		result.add(new FractalLine(308.16934f, 463.0912f, 351.67804f, 475.11188f));
		result.add(new FractalLine(379.46878f, 509.38522f, 340.68793f, 534.1932f));
		result.add(new FractalLine(370.7709f, 150.76637f, 361.96658f, 125.46217f));
		result.add(new FractalLine(372.8886f, 153.77618f, 398.08536f, 164.34918f));
		result.add(new FractalLine(116.61423f, 418.45987f, 129.04785f, 394.19342f));
		result.add(new FractalLine(543.91876f, 394.5104f, 553.8491f, 420.48584f));
		result.add(new FractalLine(209.06712f, 277.7657f, 221.50076f, 253.4992f));
		result.add(new FractalLine(227.5339f, 590.87305f, 204.33434f, 576.6589f));
		result.add(new FractalLine(337.515f, 361.10275f, 364.1137f, 353.1956f));
		result.add(new FractalLine(432.95062f, 308.19427f, 442.881f, 334.16968f));
		result.add(new FractalLine(274.81543f, 370.43076f, 301.4141f, 362.52362f));
		result.add(new FractalLine(472.49518f, 547.2572f, 460.35904f, 572.8242f));
		result.add(new FractalLine(286.8755f, 206.77573f, 299.3091f, 182.50926f));
		result.add(new FractalLine(162.81264f, 508.06485f, 139.61308f, 493.85065f));
		result.add(new FractalLine(444.4532f, 368.4645f, 471.05185f, 360.5574f));
		result.add(new FractalLine(386.99802f, 138.32735f, 411.61148f, 127.599205f));
		result.add(new FractalLine(102.315125f, 403.24817f, 93.510796f, 377.944f));
		result.add(new FractalLine(565.2061f, 394.0347f, 590.40295f, 404.6077f));
		result.add(new FractalLine(194.76802f, 262.554f, 185.9637f, 237.24976f));
		result.add(new FractalLine(211.31279f, 603.94446f, 185.4675f, 610.7823f));
		result.add(new FractalLine(338.65872f, 339.88666f, 351.09235f, 315.62018f));
		result.add(new FractalLine(454.23798f, 307.71857f, 479.43475f, 318.2916f));
		result.add(new FractalLine(275.95914f, 349.2147f, 288.3928f, 324.9482f));
		result.add(new FractalLine(487.80872f, 562.5891f, 497.73904f, 588.5646f));
		result.add(new FractalLine(272.5764f, 191.56401f, 263.77206f, 166.25983f));
		result.add(new FractalLine(146.59152f, 521.1363f, 120.74624f, 527.97406f));
		result.add(new FractalLine(445.5969f, 347.24847f, 458.03052f, 322.982f));
		result.add(new FractalLine(239.04436f, 380.44208f, 213.19907f, 387.2798f));
		result.add(new FractalLine(331.96927f, 568.8465f, 336.80923f, 595.0808f));
		result.add(new FractalLine(113.45873f, 420.33826f, 87.61345f, 427.17603f));
		result.add(new FractalLine(544.64526f, 390.83618f, 557.07886f, 366.56967f));
		result.add(new FractalLine(283.02292f, 455.33838f, 259.82336f, 441.12418f));
		result.add(new FractalLine(205.91162f, 279.64407f, 180.06633f, 286.48184f));
		result.add(new FractalLine(334.62878f, 260.9323f, 347.06244f, 236.66583f));
		result.add(new FractalLine(229.16312f, 594.15533f, 234.00316f, 620.3895f));
		result.add(new FractalLine(220.32327f, 464.66632f, 197.12372f, 450.45212f));
		result.add(new FractalLine(438.16415f, 441.67624f, 464.76282f, 433.7691f));
		result.add(new FractalLine(255.26552f, 367.37067f, 232.06598f, 353.15643f));
		result.add(new FractalLine(400.31906f, 238.22835f, 425.51584f, 248.80136f));
		result.add(new FractalLine(320.19595f, 551.71436f, 304.2912f, 573.7171f));
		result.add(new FractalLine(202.72382f, 397.58295f, 215.15746f, 373.31647f));
		result.add(new FractalLine(304.04504f, 458.08792f, 298.19827f, 431.02283f));
		result.add(new FractalLine(333.91452f, 360.10086f, 310.71497f, 345.88666f));
		result.add(new FractalLine(501.04846f, 474.06137f, 510.97885f, 500.0368f));
		result.add(new FractalLine(333.48508f, 282.14838f, 360.08374f, 274.24124f));
		result.add(new FractalLine(295.17673f, 256.88876f, 307.61035f, 232.62231f));
		result.add(new FractalLine(241.34538f, 467.4159f, 235.49864f, 440.3508f));
		result.add(new FractalLine(421.7464f, 455.74792f, 446.26254f, 469.76483f));
		result.add(new FractalLine(433.6771f, 304.52005f, 446.11072f, 280.25357f));
		result.add(new FractalLine(271.21494f, 369.42886f, 248.01541f, 355.21466f));
		result.add(new FractalLine(475.7098f, 545.20917f, 502.30853f, 537.302f));
		result.add(new FractalLine(283.71997f, 208.65411f, 257.8747f, 215.49185f));
		result.add(new FractalLine(213.30333f, 503.61215f, 190.10379f, 489.39795f));
		result.add(new FractalLine(164.44185f, 511.3471f, 169.28188f, 537.58136f));
		result.add(new FractalLine(379.03174f, 238.70404f, 388.9621f, 264.67947f));
		result.add(new FractalLine(440.8527f, 367.46262f, 417.65317f, 353.2484f));
		result.add(new FractalLine(413.40805f, 409.7995f, 440.0067f, 401.89236f));
		result.add(new FractalLine(256.8947f, 370.6529f, 261.7347f, 396.88718f));
		result.add(new FractalLine(390.08035f, 387.74524f, 400.0107f, 413.72067f));
		result.add(new FractalLine(323.58542f, 550.34265f, 350.05496f, 547.51984f));
		result.add(new FractalLine(350.7084f, 419.1275f, 377.30704f, 411.22037f));
		result.add(new FractalLine(302.7747f, 461.59412f, 286.87f, 483.59686f));
		result.add(new FractalLine(383.81552f, 571.6257f, 371.67944f, 597.1927f));
		result.add(new FractalLine(201.5801f, 418.799f, 228.17877f, 410.89188f));
		result.add(new FractalLine(330.72534f, 160.56432f, 321.921f, 135.26013f));
		result.add(new FractalLine(329.88458f, 281.14648f, 306.68506f, 266.93228f));
		result.add(new FractalLine(120.165344f, 460.92603f, 94.32007f, 467.7638f));
		result.add(new FractalLine(240.07504f, 470.9221f, 224.1703f, 492.9248f));
		result.add(new FractalLine(485.73492f, 458.72946f, 473.59882f, 484.29648f));
		result.add(new FractalLine(419.95288f, 452.3939f, 414.10608f, 425.32883f));
		result.add(new FractalLine(294.03302f, 278.1048f, 320.63165f, 270.1977f));
		result.add(new FractalLine(379.75818f, 235.02985f, 392.1918f, 210.76337f));
		result.add(new FractalLine(234.32544f, 506.36172f, 228.4787f, 479.29663f));
		result.add(new FractalLine(197.97963f, 417.79712f, 174.78009f, 403.58292f));
		result.add(new FractalLine(396.9903f, 423.87112f, 421.50638f, 437.8881f));
		result.add(new FractalLine(374.7668f, 372.41333f, 362.6307f, 397.98035f));
		result.add(new FractalLine(334.29065f, 433.19913f, 358.80676f, 447.21606f));
		result.add(new FractalLine(384.3346f, 549.5786f, 356.98798f, 558.6189f));
		result.add(new FractalLine(509.28537f, 368.2522f, 521.71906f, 343.98572f));
		result.add(new FractalLine(345.02444f, 175.77605f, 357.45807f, 151.50955f));
		result.add(new FractalLine(212.61821f, 320.2318f, 186.77293f, 327.06955f));
		result.add(new FractalLine(136.38647f, 447.85458f, 113.18692f, 433.6404f));
		result.add(new FractalLine(270.05545f, 590.5657f, 274.89542f, 616.7999f));
		result.add(new FractalLine(508.14166f, 389.4683f, 534.74036f, 381.56116f));
		result.add(new FractalLine(308.75854f, 393.56677f, 285.559f, 379.35257f));
		result.add(new FractalLine(228.83936f, 307.16037f, 205.63982f, 292.94614f));
		result.add(new FractalLine(398.31726f, 281.9361f, 410.7509f, 257.66962f));
		result.add(new FractalLine(488.94955f, 456.6814f, 515.5483f, 448.7743f));
		result.add(new FractalLine(258.28214f, 573.4335f, 242.37743f, 595.4362f));
		result.add(new FractalLine(246.05894f, 402.89478f, 222.85938f, 388.68057f));
		result.add(new FractalLine(329.78067f, 396.31635f, 323.9339f, 369.25125f));
		result.add(new FractalLine(467.47495f, 503.31146f, 494.0736f, 495.40433f));
		result.add(new FractalLine(290.43253f, 277.1029f, 267.23303f, 262.8887f));
		result.add(new FractalLine(290.4266f, 249.24185f, 264.58133f, 256.0796f));
		result.add(new FractalLine(397.17352f, 303.15216f, 423.7722f, 295.24503f));
		result.add(new FractalLine(233.0551f, 509.86792f, 217.15038f, 531.8706f));
		result.add(new FractalLine(205.33418f, 507.75745f, 210.17416f, 533.9917f));
		result.add(new FractalLine(415.69672f, 400.9285f, 392.49713f, 386.7143f));
		result.add(new FractalLine(297.78705f, 367.0633f, 302.627f, 393.29755f));
		result.add(new FractalLine(395.19675f, 420.51712f, 389.35f, 393.45203f));
		result.add(new FractalLine(267.08105f, 405.64435f, 261.2343f, 378.57925f));
		result.add(new FractalLine(377.98145f, 370.36526f, 404.58017f, 362.45816f));
		result.add(new FractalLine(451.0572f, 517.3831f, 475.57327f, 531.4001f));
		result.add(new FractalLine(323.12006f, 509.38406f, 349.58957f, 506.56122f));
		result.add(new FractalLine(306.64774f, 236.17041f, 283.4482f, 221.95619f));
		result.add(new FractalLine(332.49713f, 429.84512f, 326.6504f, 402.78006f));
		result.add(new FractalLine(193.56088f, 490.62524f, 177.65614f, 512.6279f));
		result.add(new FractalLine(436.7188f, 403.67807f, 430.87204f, 376.61298f));
		result.add(new FractalLine(334.16083f, 489.16522f, 318.2561f, 511.16797f));
		result.add(new FractalLine(286.01376f, 349.9311f, 270.10904f, 371.9338f));
		result.add(new FractalLine(304.7286f, 314.61237f, 281.52902f, 300.39816f));
		result.add(new FractalLine(305.18005f, 519.7966f, 325.8636f, 537.29236f));
		result.add(new FractalLine(271.46118f, 498.49323f, 255.55644f, 520.4959f));
		result.add(new FractalLine(338.49338f, 468.45807f, 311.12076f, 472.21945f));
		result.add(new FractalLine(377.64108f, 457.40466f, 371.7943f, 430.3396f));
		result.add(new FractalLine(325.7507f, 317.36194f, 319.90393f, 290.29684f));
		result.add(new FractalLine(275.79373f, 477.78607f, 248.42113f, 481.54742f));
		result.add(new FractalLine(390.39462f, 474.80844f, 406.2031f, 451.4808f));
		result.add(new FractalLine(344.39838f, 212.44586f, 356.832f, 188.17938f));
		result.add(new FractalLine(343.25464f, 233.66191f, 369.8533f, 225.75478f));
		result.add(new FractalLine(172.82365f, 451.26297f, 149.62411f, 437.04877f));
		result.add(new FractalLine(193.84576f, 454.01254f, 187.99902f, 426.94745f));
		result.add(new FractalLine(480.7147f, 414.7837f, 507.31335f, 406.87656f));
		result.add(new FractalLine(464.29694f, 428.85535f, 488.81302f, 442.8723f));
		result.add(new FractalLine(265.27655f, 310.5688f, 242.077f, 296.35458f));
		result.add(new FractalLine(286.29868f, 313.31836f, 280.4519f, 286.25327f));
		result.add(new FractalLine(264.44122f, 537.4391f, 248.5365f, 559.44183f));
		result.add(new FractalLine(268.77377f, 516.7319f, 241.40115f, 520.4933f));
		result.add(new FractalLine(365.6385f, 442.93164f, 381.44696f, 419.604f));
		result.add(new FractalLine(353.3288f, 342.53925f, 377.84494f, 356.55615f));
		result.add(new FractalLine(302.93887f, 452.25964f, 318.7473f, 428.932f));
		result.add(new FractalLine(352.88498f, 425.52786f, 347.0382f, 398.4628f));
		result.add(new FractalLine(388.10153f, 550.50476f, 412.61765f, 564.5217f));
		result.add(new FractalLine(391.25092f, 512.8022f, 398.16827f, 540.6975f));
		result.add(new FractalLine(357.0941f, 188.23816f, 367.02448f, 214.21361f));
		result.add(new FractalLine(149.70251f, 436.79245f, 176.30118f, 428.8853f));
		result.add(new FractalLine(507.45435f, 407.11084f, 495.31824f, 432.6779f));
		result.add(new FractalLine(242.1554f, 296.09824f, 268.75406f, 288.19113f));
		result.add(new FractalLine(248.28735f, 559.3443f, 242.44058f, 532.2793f));
		result.add(new FractalLine(347.28198f, 398.34033f, 371.7981f, 412.35727f));
		result.add(new FractalLine(396.48624f, 320.7947f, 384.35013f, 346.36172f));
		result.add(new FractalLine(284.58237f, 407.66833f, 309.0985f, 421.68527f));
		result.add(new FractalLine(437.59015f, 529.2787f, 410.24347f, 538.319f));
		result.add(new FractalLine(319.96378f, 225.10829f, 346.56244f, 217.20116f));
		result.add(new FractalLine(183.5661f, 476.5361f, 177.71936f, 449.47104f));
		result.add(new FractalLine(454.22012f, 405.70206f, 478.73624f, 419.719f));
		result.add(new FractalLine(276.019f, 335.84195f, 270.17224f, 308.7769f));
		result.add(new FractalLine(290.4028f, 528.67065f, 263.0302f, 532.432f));
		result.add(new FractalLine(341.86893f, 177.65443f, 316.02365f, 184.4922f));
		result.add(new FractalLine(341.83658f, 451.19885f, 357.64505f, 427.87122f));
		result.add(new FractalLine(343.252f, 319.38593f, 367.76813f, 333.40286f));
		result.add(new FractalLine(369.74655f, 328.46756f, 396.3452f, 320.56042f));
		result.add(new FractalLine(279.13693f, 460.52682f, 294.94537f, 437.1992f));
		result.add(new FractalLine(290.18536f, 434.8559f, 284.3386f, 407.79083f));
		result.add(new FractalLine(138.01569f, 451.13684f, 142.8557f, 477.3711f));
		result.add(new FractalLine(413.14728f, 514.99335f, 437.6634f, 529.0103f));
		result.add(new FractalLine(401.20718f, 489.1111f, 408.12457f, 517.0065f));
		result.add(new FractalLine(378.38144f, 187.76247f, 403.57822f, 198.33548f));
		result.add(new FractalLine(504.54117f, 388.4664f, 481.3416f, 374.25217f));
		result.add(new FractalLine(150.84622f, 415.5764f, 163.27986f, 391.3099f));
		result.add(new FractalLine(342.56732f, 251.30444f, 330.4312f, 276.8715f));
		result.add(new FractalLine(522.7679f, 422.44275f, 532.6983f, 448.41818f));
		result.add(new FractalLine(230.46855f, 310.44263f, 235.30855f, 336.67685f));
		result.add(new FractalLine(243.29912f, 274.8822f, 255.73276f, 250.61574f));
		result.add(new FractalLine(211.34706f, 456.03653f, 235.86317f, 470.05347f));
		result.add(new FractalLine(227.26526f, 556.5948f, 204.06573f, 542.3806f));
		result.add(new FractalLine(261.6716f, 572.06177f, 288.14108f, 569.239f));
		result.add(new FractalLine(363.69974f, 384.26865f, 390.29843f, 376.3615f));
		result.add(new FractalLine(450.8299f, 440.75095f, 423.48328f, 449.7912f));
		result.add(new FractalLine(411.79977f, 336.1266f, 421.73013f, 362.102f));
		result.add(new FractalLine(301.00012f, 393.59665f, 327.59885f, 385.68954f));
		result.add(new FractalLine(328.5103f, 399.82254f, 312.6056f, 421.82526f));
		result.add(new FractalLine(437.07108f, 551.3258f, 424.93497f, 576.8928f));
		result.add(new FractalLine(303.79993f, 315.34235f, 328.31604f, 329.35928f));
		result.add(new FractalLine(321.10748f, 203.89223f, 333.54114f, 179.62575f));
		result.add(new FractalLine(393.57303f, 302.15027f, 370.3735f, 287.9361f));
		result.add(new FractalLine(162.544f, 473.78653f, 139.34448f, 459.57236f));
		result.add(new FractalLine(265.81073f, 409.15054f, 249.90598f, 431.1533f));
		result.add(new FractalLine(272.11697f, 499.47266f, 287.9254f, 476.14502f));
		result.add(new FractalLine(376.45105f, 457.2343f, 383.36844f, 485.12967f));
		result.add(new FractalLine(449.26364f, 514.0291f, 443.41687f, 486.96405f));
		result.add(new FractalLine(470.63788f, 391.63037f, 497.23654f, 383.72324f));
		result.add(new FractalLine(254.9969f, 333.09238f, 231.79738f, 318.87817f));
		result.add(new FractalLine(308.27695f, 239.45265f, 313.11694f, 265.68692f));
		result.add(new FractalLine(286.07028f, 549.3778f, 270.1656f, 571.3805f));
		result.add(new FractalLine(339.8618f, 354.43484f, 312.51517f, 363.47513f));
		result.add(new FractalLine(329.08304f, 433.79507f, 323.23627f, 406.73f));
		result.add(new FractalLine(196.95033f, 489.25357f, 223.41985f, 486.43082f));
		result.add(new FractalLine(359.66977f, 305.31424f, 386.26846f, 297.4071f));
		result.add(new FractalLine(313.75143f, 466.56232f, 320.66882f, 494.45767f));
		result.add(new FractalLine(266.38342f, 443.12308f, 260.53665f, 416.058f));
		result.add(new FractalLine(435.4485f, 407.18427f, 419.54373f, 429.187f));
		result.add(new FractalLine(423.10358f, 491.3023f, 447.61972f, 505.31924f));
		result.add(new FractalLine(373.03012f, 511.25296f, 347.15424f, 497.4821f));
		result.add(new FractalLine(357.88086f, 266.63635f, 367.81125f, 292.6118f));
		result.add(new FractalLine(227.76479f, 441.96487f, 254.36345f, 434.05774f));
		result.add(new FractalLine(289.4032f, 348.55942f, 315.87274f, 345.73666f));
		result.add(new FractalLine(450.31082f, 462.79803f, 438.1747f, 488.36502f));
		result.add(new FractalLine(320.21768f, 301.2707f, 346.8163f, 293.36356f));
		result.add(new FractalLine(304.07275f, 516.32f, 303.27484f, 489.7697f));
		result.add(new FractalLine(341.88538f, 469.98834f, 362.56894f, 487.48416f));
		result.add(new FractalLine(324.48038f, 320.86813f, 308.57562f, 342.87088f));
		result.add(new FractalLine(279.18573f, 479.31635f, 299.86926f, 496.81216f));
		result.add(new FractalLine(386.9213f, 476.33813f, 359.54868f, 480.09952f));
		result.add(new FractalLine(339.65414f, 232.66002f, 316.45462f, 218.44582f));
		result.add(new FractalLine(192.57542f, 457.51874f, 176.6707f, 479.52145f));
		result.add(new FractalLine(462.5034f, 425.5013f, 456.65662f, 398.43628f));
		result.add(new FractalLine(285.02832f, 316.82455f, 269.1236f, 338.82727f));
		result.add(new FractalLine(272.16577f, 518.26215f, 292.84933f, 535.75793f));
		result.add(new FractalLine(362.16516f, 444.46133f, 334.79254f, 448.22272f));
		result.add(new FractalLine(259.36346f, 482.0689f, 253.5167f, 455.00385f));
		result.add(new FractalLine(398.34744f, 459.4255f, 422.8636f, 473.44244f));
		result.add(new FractalLine(351.53528f, 339.1852f, 345.68848f, 312.12015f));
		result.add(new FractalLine(339.3427f, 376.48193f, 327.20663f, 402.04892f));
		result.add(new FractalLine(335.64783f, 468.7535f, 360.16397f, 482.77045f));
		result.add(new FractalLine(356.43985f, 526.3685f, 329.0932f, 535.40875f));
		result.add(new FractalLine(299.46555f, 453.78934f, 272.09296f, 457.55072f));
		result.add(new FractalLine(392.45856f, 509.1246f, 408.26703f, 485.79694f));
		result.add(new FractalLine(357.82056f, 184.56396f, 370.25418f, 160.29747f));
		result.add(new FractalLine(146.10205f, 435.79056f, 122.90249f, 421.5764f));
		result.add(new FractalLine(510.669f, 405.06277f, 537.2677f, 397.15564f));
		result.add(new FractalLine(238.55493f, 295.09634f, 215.35541f, 280.88214f));
		result.add(new FractalLine(247.01703f, 562.8505f, 231.11232f, 584.8533f));
		result.add(new FractalLine(345.48843f, 394.9863f, 339.64166f, 367.9212f));
		result.add(new FractalLine(399.70087f, 318.74664f, 426.29956f, 310.8395f));
		result.add(new FractalLine(282.78882f, 404.3143f, 276.94208f, 377.2492f));
		result.add(new FractalLine(441.3571f, 530.2049f, 465.87323f, 544.2218f));
		result.add(new FractalLine(316.36328f, 224.1064f, 293.16376f, 209.89218f));
		result.add(new FractalLine(182.29576f, 480.0423f, 166.39102f, 502.04498f));
		result.add(new FractalLine(452.42657f, 402.34802f, 446.57977f, 375.28296f));
		result.add(new FractalLine(274.74866f, 339.34814f, 258.84393f, 361.35083f));
		result.add(new FractalLine(293.7948f, 530.2009f, 314.47833f, 547.6967f));
		result.add(new FractalLine(338.36325f, 452.72855f, 310.99063f, 456.48993f));
		result.add(new FractalLine(341.45847f, 316.0319f, 335.6117f, 288.9668f));
		result.add(new FractalLine(275.6636f, 462.05652f, 248.29102f, 465.8179f));
		result.add(new FractalLine(402.41483f, 485.43356f, 418.22327f, 462.10593f));
		result.add(new FractalLine(345.78195f, 249.25638f, 372.38068f, 241.34926f));
		result.add(new FractalLine(209.55353f, 452.6825f, 203.70676f, 425.61746f));
		result.add(new FractalLine(454.59683f, 441.6771f, 479.11295f, 455.69406f));
		result.add(new FractalLine(302.0064f, 311.9883f, 296.15964f, 284.92328f));
		result.add(new FractalLine(268.64365f, 501.00235f, 241.27106f, 504.76373f));
		result.add(new FractalLine(377.6587f, 453.55676f, 393.46716f, 430.22913f));
		result.add(new FractalLine(343.62872f, 355.361f, 368.14484f, 369.37793f));
		result.add(new FractalLine(314.95908f, 462.88477f, 330.76752f, 439.55713f));
		result.add(new FractalLine(374.99637f, 514.6762f, 381.91376f, 542.5716f));

		return result;
	}
}
