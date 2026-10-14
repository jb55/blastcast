//! Artwork images for the artwork role: a group's image, scaled for each
//! channel a client declared.
//!
//! The spec has the server deliver every image at exactly the channel's
//! width by height: scaled to fit, aspect ratio kept, the rest padded with
//! black, never cropped. JPEG and PNG are the formats a server must offer.

use std::io::Cursor;

use bytes::Bytes;
use image::codecs::jpeg::JpegEncoder;
use image::imageops::{self, FilterType};
use image::{DynamicImage, ImageFormat, Rgb, RgbImage};

/// The largest side this server renders; a bigger request is refused
/// rather than allocated.
pub const MAX_SIDE: u32 = 4096;
const JPEG_QUALITY: u8 = 85;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageKind {
    Jpeg,
    Png,
}

impl ImageKind {
    /// The kind a channel's `format` names, if this server can encode it.
    pub fn parse(format: &str) -> Option<Self> {
        match format {
            "jpeg" => Some(Self::Jpeg),
            "png" => Some(Self::Png),
            _ => None,
        }
    }
}

/// What one channel is sent: its format and exact size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Target {
    pub kind: ImageKind,
    pub width: u32,
    pub height: u32,
}

impl Target {
    /// The target for a channel's `format`, `width` and `height`, unless the
    /// format is one this server can't encode or the size is 0 or above
    /// [`MAX_SIDE`].
    pub fn new(format: &str, width: u32, height: u32) -> Option<Self> {
        let kind = ImageKind::parse(format)?;
        let side = 1..=MAX_SIDE;
        (side.contains(&width) && side.contains(&height)).then_some(Self {
            kind,
            width,
            height,
        })
    }
}

/// The size an image of `width` by `height` scales to inside a `box_w` by
/// `box_h` box, aspect ratio kept. Never 0 on a side.
pub fn fit(width: u32, height: u32, box_w: u32, box_h: u32) -> (u32, u32) {
    let (w, h, bw, bh) = (
        u64::from(width.max(1)),
        u64::from(height.max(1)),
        u64::from(box_w),
        u64::from(box_h),
    );
    // Width-bound when the image is wider than the box, relative to height.
    let (fw, fh) = if w * bh >= h * bw {
        (bw, (h * bw + w / 2) / w)
    } else {
        ((w * bh + h / 2) / h, bh)
    };
    (fw.clamp(1, bw) as u32, fh.clamp(1, bh) as u32)
}

/// Renders `image` (any JPEG or PNG) for each target, decoding it once. A
/// target is `None` if it can't be encoded; all are when the image can't be
/// decoded.
pub fn render(image: &[u8], targets: &[Target]) -> Vec<Option<Bytes>> {
    let source = match image::load_from_memory(image) {
        Ok(source) => source,
        Err(e) => {
            tracing::warn!(error = %e, len = image.len(), "can't decode artwork");
            return vec![None; targets.len()];
        }
    };
    targets
        .iter()
        .map(|&target| match render_one(&source, target) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                tracing::warn!(error = %e, ?target, "can't encode artwork");
                None
            }
        })
        .collect()
}

fn render_one(source: &DynamicImage, target: Target) -> image::ImageResult<Bytes> {
    let (w, h) = fit(source.width(), source.height(), target.width, target.height);
    let scaled = source.resize_exact(w, h, FilterType::Lanczos3).to_rgb8();
    let mut canvas = RgbImage::from_pixel(target.width, target.height, Rgb([0, 0, 0]));
    let x = (target.width - w) / 2;
    let y = (target.height - h) / 2;
    imageops::overlay(&mut canvas, &scaled, x.into(), y.into());
    let mut out = Vec::new();
    match target.kind {
        ImageKind::Jpeg => {
            canvas.write_with_encoder(JpegEncoder::new_with_quality(&mut out, JPEG_QUALITY))?
        }
        ImageKind::Png => canvas.write_to(&mut Cursor::new(&mut out), ImageFormat::Png)?,
    }
    Ok(out.into())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A `w` by `h` PNG, white.
    pub(crate) fn white_png(w: u32, h: u32) -> Vec<u8> {
        let img = RgbImage::from_pixel(w, h, Rgb([255, 255, 255]));
        let mut out = Vec::new();
        img.write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
            .unwrap();
        out
    }

    #[test]
    fn fits_inside_the_box_keeping_the_aspect_ratio() {
        assert_eq!(fit(1000, 1000, 300, 300), (300, 300));
        assert_eq!(fit(1000, 500, 300, 300), (300, 150));
        assert_eq!(fit(500, 1000, 300, 300), (150, 300));
        assert_eq!(fit(100, 100, 800, 400), (400, 400), "scales up too");
        assert_eq!(fit(4000, 1, 100, 100), (100, 1), "never 0");
        assert_eq!(fit(0, 0, 10, 20), (10, 10));
    }

    #[test]
    fn targets_refuse_what_cant_be_made() {
        assert!(Target::new("jpeg", 300, 300).is_some());
        assert!(Target::new("png", 1, MAX_SIDE).is_some());
        assert!(Target::new("bmp", 300, 300).is_none());
        assert!(Target::new("jpeg", 0, 300).is_none());
        assert!(Target::new("jpeg", MAX_SIDE + 1, 300).is_none());
    }

    #[test]
    fn renders_exactly_the_size_asked_with_black_bars() {
        let wide = white_png(200, 100);
        let targets = [
            Target::new("png", 100, 100).unwrap(),
            Target::new("jpeg", 64, 32).unwrap(),
        ];
        let out = render(&wide, &targets);

        let png = image::load_from_memory(out[0].as_ref().unwrap())
            .unwrap()
            .to_rgb8();
        assert_eq!(png.dimensions(), (100, 100));
        assert_eq!(png.get_pixel(50, 10), &Rgb([0, 0, 0]), "bar above");
        assert_eq!(png.get_pixel(50, 50), &Rgb([255, 255, 255]), "image");
        assert_eq!(png.get_pixel(50, 90), &Rgb([0, 0, 0]), "bar below");

        let jpeg = out[1].as_ref().unwrap();
        assert_eq!(&jpeg[..2], [0xff, 0xd8], "a JPEG");
        let jpeg = image::load_from_memory(jpeg).unwrap();
        assert_eq!((jpeg.width(), jpeg.height()), (64, 32));
    }

    #[test]
    fn an_undecodable_image_renders_nothing() {
        let targets = [Target::new("png", 10, 10).unwrap()];
        assert_eq!(render(b"not an image", &targets), [None]);
    }
}
