pub type ResourceId = String;
pub type IndexRange = (usize, usize);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Default)]
pub struct Tuple {
    pub tuple_id: ResourceId,
    pub index: Vec<i32>,
    pub values: Vec<i32>,
}

impl Tuple {
    pub fn empty_tuple(idx_slice: &[i32]) -> Self {
        Self {
            tuple_id: idx_slice
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
            index: Vec::from(idx_slice),
            values: vec![],
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct DataChunk {
    chunk_id: ResourceId,
    start: usize,
    end: usize,
    seq_data: Vec<i32>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct DataChunkIndex {
    index: usize,
    value_index: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Segment {
    segment_id: ResourceId,
    capacity: usize,
    capacity_per_chunk: usize,
    chunks: Vec<DataChunk>,
}

impl Segment {
    pub fn from_ints(capacity_per_chunk: usize, ints: &[i32], segment_id: String) -> Self {
        let mut index = 0;
        let mut chunks = vec![];
        let mut is_break = false;
        loop {
            let start = index * capacity_per_chunk;
            let end = if start + capacity_per_chunk >= ints.len() {
                is_break = true;
                ints.len() - 1
            } else {
                start + capacity_per_chunk
            };
            chunks.push(DataChunk {
                chunk_id: [start.to_string(), end.to_string()].join(","),
                start,
                end,
                seq_data: ints[start..end].to_owned(),
            });
            index += 1;
            if is_break {
                break;
            }
        }
        Self {
            segment_id,
            capacity: ints.len(),
            capacity_per_chunk,
            chunks,
        }
    }

    pub fn chunks(&self) -> Vec<DataChunk> {
        self.chunks.clone()
    }

    pub fn capacity(&self) -> i32 {
        self.capacity as i32
    }

    pub fn get_chunk(&self, idx: usize) -> Option<&DataChunk> {
        for chunk in self.chunks.iter() {
            if chunk.end > idx {
                return Some(chunk);
            }
        }
        self.chunks.last()
    }

    pub fn update_value(&mut self, index: usize, new_value: i32) {
        let chunk_index_vec = self.get_chunk_index(&[index as i32]);
        assert_eq!(chunk_index_vec.len(), 1);
        let chunk_index = &chunk_index_vec[0];
        self.chunks[chunk_index.index].seq_data[chunk_index.value_index] = new_value;
    }

    fn get_chunk_index(&self, index: &[i32]) -> Vec<DataChunkIndex> {
        let mut data = vec![];
        index.iter().for_each(|idx| {
            let chunk_opt = self.get_chunk(*idx as usize);
            if let Some(chunk) = chunk_opt {
                data.push(DataChunkIndex {
                    index: *idx as usize,
                    value_index: *idx as usize - chunk.start as usize,
                });
            }
        });
        data
    }

    pub fn get_tuple(&self, index: &[i32]) -> Tuple {
        let mut seq_vals = vec![];
        index.iter().for_each(|idx| {
            let chunk_opt = self.get_chunk(*idx as usize);
            if let Some(chunk) = chunk_opt {
                seq_vals.push(chunk.seq_data[*idx as usize - chunk.start as usize])
            }
        });
        Tuple {
            tuple_id: index
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(","),
            index: Vec::from(index),
            values: seq_vals,
        }
    }
}
