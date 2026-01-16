// Sphere Live Stream Player
// Copyright (C) 2015 progre (djyayutto@gmail.com)

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
import Title from './title';

export function attach(element: HTMLElement, video: HTMLVideoElement, title: Title) {
    element.addEventListener('wheel', event => {
        // Windowsのデフォルトでは+-100 OSXではしらん
        let delta = 0.05;
        if (event.deltaY > 0) {
            if (video.volume - delta < 0) {
                video.volume = 0;
            } else {
                video.volume -= delta;
            }
        } else {
            if (1 < video.volume + delta) {
                video.volume = 1;
            } else {
                video.volume += delta;
            }
        }
        title.setVolume(video.volume);
    });
}